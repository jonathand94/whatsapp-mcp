// fetch_url.go — the third way a draft's bytes can arrive: a URL the bridge
// downloads itself.
//
// file_path and file_base64 both fail for a remote client. base64 routes the
// bytes through the model's context, which runs out around a hundred kilobytes;
// and a client on claude.ai has no filesystem on this machine to name. So a
// file the user already has in Drive is unreachable, which is the common case.
//
// Downloading server-side fixes that, and in doing so hands the bridge a
// capability it did not have: making requests to a host the *caller* chose.
// That is the definition of SSRF, so the guard below is not an add-on, it is
// the substance of the feature. Four properties carry it, in this order:
//
//  1. Every hop is checked, not only the URL that was passed in. A 302 to
//     169.254.169.254 would otherwise put instance-metadata credentials into a
//     WhatsApp message.
//  2. The address actually dialed is checked, not the hostname. DNS is under
//     the caller's control and may answer differently the second time.
//  3. The size cap is applied while reading. Content-Length is a claim.
//  4. text/html is refused. Drive answers a login page with 200 OK for a file
//     that is not public; without this the recipient receives that page
//     dressed up as a photo.
//
// The body streams to the caller's writer rather than into memory, so a URL
// can carry anything up to WhatsApp's 2 GB document ceiling.

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// maxFetchRedirects allows the hops a real download needs — Drive uses two
	// or three — and stops well short of a redirect loop.
	maxFetchRedirects = 5

	// fetchTotalTimeout is a backstop for the whole download, sized for 2 GB
	// over a slow link. A download that stops moving is abandoned long before
	// it, after stallTimeout (jobs.go).
	fetchTotalTimeout  = mediaJobTimeout
	fetchDialTimeout   = 15 * time.Second
	fetchHeaderTimeout = 30 * time.Second

	// Some hosts serve a different (or no) body to a client that does not look
	// like a browser. Naming the bridge as well keeps the request honest.
	fetchUserAgent = "Mozilla/5.0 (compatible; whatsapp-mcp-bridge)"
)

// errBlockedTarget marks every refusal that comes from the address guard, so a
// test can assert the guard fired rather than matching on message text.
var errBlockedTarget = errors.New("destino no permitido")

// errHTMLBody is the one download failure the user can act on directly, so it
// says what to do rather than what happened.
var errHTMLBody = errors.New(
	"file_url devolvio una pagina HTML en lugar de un archivo; si es un enlace de Google Drive, " +
		"compartelo como \"cualquier persona con el enlace\" y vuelve a intentar")

// cgnatRange is carrier-grade NAT. Not private under RFC 1918, but it is still
// "inside a network someone else is running", which is what the guard is about.
var cgnatRange = netip.MustParsePrefix("100.64.0.0/10")

// addrPolicy decides whether an address may be dialed.
//
// It is a parameter rather than a constant so the tests can serve from
// 127.0.0.1 and still reach the streaming, HTML and naming logic behind the
// guard. A nil policy means the strict one, so there is no production switch
// that weakens it; the permissive policy exists only in the test file, and even
// that one defers to blockedIP for every address other than loopback.
type addrPolicy func(netip.Addr) (blocked bool, why string)

func (p addrPolicy) orStrict() addrPolicy {
	if p == nil {
		return blockedIP
	}
	return p
}

// fetcher carries the two halves of a download that have to agree: the address
// policy that decides where it may go, and the client that enforces it.
//
// The zero value is the production fetcher — strict policy, guarded transport —
// so `fetcher{}` at a call site is the safe thing to write and the short thing
// to write at the same time. A test that needs to reach a local server
// substitutes both together rather than weakening one of them.
type fetcher struct {
	policy addrPolicy   // nil → blockedIP
	client *http.Client // nil → newFetchClient(policy)
}

func (f fetcher) httpClient() *http.Client {
	if f.client == nil {
		return newFetchClient(f.policy)
	}
	return f.client
}

// blockedIP reports whether an address is one the bridge must never be talked
// into fetching from.
//
// Deny-by-range rather than allow-by-range: the public internet has no stable
// allowlist, while the ranges that mean "somewhere inside this network" are a
// short, fixed list.
func blockedIP(addr netip.Addr) (bool, string) {
	// ::ffff:169.254.169.254 is the metadata address wearing a hat. Unmapping
	// first is what makes the rest of this function correct for v6 callers.
	addr = addr.Unmap()

	switch {
	case !addr.IsValid():
		return true, "direccion invalida"
	case addr.IsLoopback():
		return true, "loopback"
	case addr.IsLinkLocalUnicast(), addr.IsLinkLocalMulticast():
		// 169.254.0.0/16 lives here, and with it every cloud provider's
		// instance-metadata endpoint. This is the case that turns a file
		// download into credential exfiltration; it must never be relaxed.
		return true, "link-local (metadata de instancia)"
	case addr.IsPrivate():
		// Covers 10/8, 172.16/12, 192.168/16 and fc00::/7 — which is where
		// AWS's IPv6 metadata address fd00:ec2::254 sits.
		return true, "rango privado"
	case addr.IsUnspecified():
		return true, "direccion no especificada"
	case addr.IsMulticast(), addr.IsInterfaceLocalMulticast():
		return true, "multicast"
	case cgnatRange.Contains(addr):
		return true, "CGNAT 100.64/10"
	}
	return false, ""
}

// validateFetchURL checks what can be judged from the URL alone.
//
// https only: an http hop can be rewritten in flight by anything on the path,
// which would defeat every check made before it. A literal address is rejected
// here as well as at the dialer, purely so the common attempt fails with a
// message that names the address instead of a connection error.
func validateFetchURL(u *url.URL, policy addrPolicy) error {
	if u.Scheme != "https" {
		return fmt.Errorf("%w: solo se aceptan URLs https, no %q", errBlockedTarget, u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("%w: la URL no tiene host", errBlockedTarget)
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if bad, why := policy.orStrict()(addr); bad {
			return fmt.Errorf("%w: %s es %s", errBlockedTarget, host, why)
		}
	}
	return nil
}

// redirectChecker returns the CheckRedirect hook for the fetch client.
//
// net/http calls this on every hop by construction, which is exactly the
// property the guard needs: validating only the caller's URL would let a
// 302 → 169.254.169.254 through, and the response body would be credentials on
// their way into a chat.
func redirectChecker(policy addrPolicy) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxFetchRedirects {
			return fmt.Errorf("%w: mas de %d redirecciones", errBlockedTarget, maxFetchRedirects)
		}
		return validateFetchURL(req.URL, policy)
	}
}

// guardedDialContext refuses to open a connection to a blocked address.
//
// The redirect hook inspects URLs; this inspects the IP being dialed. Both are
// needed, because a hostname is not an address: a name can resolve inward on
// the first answer, or resolve differently between the check and the connection
// (DNS rebinding). Once an address is vetted, the connection is made to that
// address rather than to the name, so there is no second lookup to poison.
//
// A host that resolves to any blocked address is refused outright rather than
// having that address skipped: a name answering with both a public and a
// private address is the signature of the attack, not a failover setup.
func guardedDialContext(policy addrPolicy) func(context.Context, string, string) (net.Conn, error) {
	check := policy.orStrict()
	base := &net.Dialer{Timeout: fetchDialTimeout, KeepAlive: 30 * time.Second}

	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("%w: direccion %q ilegible", errBlockedTarget, address)
		}
		addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("no se pudo resolver %q: %w", host, err)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("%w: %q no resolvio a ninguna direccion", errBlockedTarget, host)
		}
		for _, addr := range addrs {
			if bad, why := check(addr); bad {
				return nil, fmt.Errorf("%w: %s resuelve a %s (%s)", errBlockedTarget, host, addr, why)
			}
		}

		var lastErr error
		for _, addr := range addrs {
			conn, err := base.DialContext(ctx, network, net.JoinHostPort(addr.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
}

// newFetchClient builds the client used for a file_url download. It is not
// http.DefaultClient and must not become it: the guard lives in the transport.
func newFetchClient(policy addrPolicy) *http.Client {
	return &http.Client{
		Timeout:       fetchTotalTimeout,
		CheckRedirect: redirectChecker(policy),
		Transport: &http.Transport{
			DialContext:           guardedDialContext(policy),
			TLSHandshakeTimeout:   fetchDialTimeout,
			ResponseHeaderTimeout: fetchHeaderTimeout,
			ForceAttemptHTTP2:     true,
		},
	}
}

// driveFileIDRE matches the id in a /file/d/<id>/view share link. Drive ids are
// long and use the URL-safe base64 alphabet; the length floor keeps this from
// matching an unrelated path segment.
var driveFileIDRE = regexp.MustCompile(`/file/d/([A-Za-z0-9_-]{10,})`)

// normalizeDriveURL rewrites a Google Drive share link into the form that
// actually serves bytes.
//
// A /file/d/<id>/view URL serves the Drive web application, so the download
// would be a page of HTML. The older /uc?export=download endpoint serves the
// file but answers with an antivirus interstitial once the file is beyond a few
// megabytes — also HTML, also not the file. drive.usercontent.google.com skips
// both, except that it too stops at a "can't scan this file for viruses" page
// for files of roughly 100 MB and up; confirm=t is the answer to that page,
// sent up front. Anything that is not a Drive link is returned untouched.
func normalizeDriveURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	switch strings.ToLower(u.Hostname()) {
	case "drive.google.com", "docs.google.com", "drive.usercontent.google.com":
	default:
		return raw
	}

	id := ""
	if m := driveFileIDRE.FindStringSubmatch(u.Path); m != nil {
		id = m[1]
	} else if q := u.Query().Get("id"); q != "" {
		id = q
	}
	if id == "" {
		return raw
	}
	return "https://drive.usercontent.google.com/download?id=" + url.QueryEscape(id) + "&export=download&confirm=t"
}

// fetchRemoteFileTo downloads a URL into dst and reports the best name it
// could find for the bytes, plus how many there were.
//
// maxBytes is the ceiling applied while reading. It is the most permissive
// limit that could apply to this draft, not the exact one: the exact limit
// depends on the content type, which is not known until the bytes are in hand.
// planDelivery applies that afterwards.
//
// dst receives the body as it arrives, so it may hold a partial download when
// this returns an error. The draft flow writes into a .part file it deletes
// on any error, which is what keeps its "nothing is accepted until the input
// is" property now that the body no longer fits in memory.
//
// job, when non-nil, learns the expected size from Content-Length.
func fetchRemoteFileTo(ctx context.Context, rawURL string, maxBytes int64, f fetcher, dst io.Writer, job *mediaJob) (string, int64, error) {
	target := normalizeDriveURL(strings.TrimSpace(rawURL))

	u, err := url.Parse(target)
	if err != nil {
		return "", 0, fmt.Errorf("file_url no es una URL valida: %w", err)
	}
	if err := validateFetchURL(u, f.policy); err != nil {
		return "", 0, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, fmt.Errorf("file_url no se pudo preparar: %w", err)
	}
	req.Header.Set("User-Agent", fetchUserAgent)

	resp, err := f.httpClient().Do(req)
	if err != nil {
		// url.Error wraps whatever the guard returned, so errors.Is still finds
		// errBlockedTarget through it.
		return "", 0, fmt.Errorf("no se pudo descargar file_url: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("file_url respondio %s", resp.Status)
	}
	if isHTML(resp.Header.Get("Content-Type")) {
		return "", 0, errHTMLBody
	}

	// Content-Length is worth a cheap early rejection and never worth trusting
	// as the enforcement point: it can be absent under chunked encoding, and it
	// can simply be wrong. It is -1 when unknown, so this comparison is safe.
	if resp.ContentLength > maxBytes {
		return "", 0, fmt.Errorf(
			"file_url declara %d bytes y el maximo es %d", resp.ContentLength, maxBytes)
	}
	if job != nil && resp.ContentLength > 0 {
		job.bytesTotal.Store(resp.ContentLength)
	}

	// Watched from the first byte: a server that sends its headers and then
	// nothing would otherwise hold the draft until the 8-hour backstop. The
	// count sits on the raw body, under the buffer, so it measures what the
	// network delivers rather than what this function has consumed.
	var moved atomic.Int64
	stop, stalled := watchStall(cancel, moved.Load, stallTimeout)
	defer stop()
	stallErr := func() error {
		return fmt.Errorf("la descarga de file_url se detuvo: no llegaron datos en %s", stallTimeout)
	}

	// A server that sent no Content-Type gets sniffed, so the HTML check cannot
	// be sidestepped by omitting the header. Peeking keeps the sniffed bytes in
	// the stream, and keeps an HTML page from ever reaching dst.
	body := bufio.NewReaderSize(&progressReader{r: resp.Body, n: &moved}, 4096)
	head, peekErr := body.Peek(512)
	if peekErr != nil && peekErr != io.EOF {
		if stalled.Load() {
			return "", 0, stallErr()
		}
		return "", 0, fmt.Errorf("descargando file_url: %w", peekErr)
	}
	if len(head) == 0 {
		return "", 0, fmt.Errorf("file_url devolvio un archivo vacio")
	}
	if isHTML(http.DetectContentType(head)) {
		return "", 0, errHTMLBody
	}

	// Read one byte past the ceiling: getting it back is the proof the body was
	// over the limit, whatever the headers said.
	n, err := io.Copy(dst, io.LimitReader(body, maxBytes+1))
	if err != nil {
		if stalled.Load() {
			return "", n, stallErr()
		}
		return "", n, fmt.Errorf("descargando file_url: %w", err)
	}
	if n > maxBytes {
		return "", n, fmt.Errorf("file_url supera el maximo de %d bytes", maxBytes)
	}

	// Name from where the bytes actually came, not where the request started: a
	// download that redirects to cdn.example.com/files/reporte.pdf names itself
	// on the last hop, and the URL the caller passed says nothing.
	named := u
	if resp.Request != nil && resp.Request.URL != nil {
		named = resp.Request.URL
	}
	return fetchNameHint(resp, named), n, nil
}

// progressReader counts bytes as they are read, for the stall watch. The
// job's own progress is counted by whatever dst the caller passed, so it is
// not counted here as well.
type progressReader struct {
	r io.Reader
	n *atomic.Int64
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.n.Add(int64(n))
	return n, err
}

func isHTML(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/html")
}

// fetchNameHint recovers the name the recipient should see.
//
// Content-Disposition is the server's own answer and beats the URL path, which
// for a normalized Drive link is only ever "download". The result is run
// through filepath.Base because it is attacker-influenced text that goes on to
// pick a file extension.
func fetchNameHint(resp *http.Response, u *url.URL) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if name := strings.TrimSpace(params["filename"]); name != "" {
				if base := filepath.Base(name); base != "." && base != string(filepath.Separator) {
					return base
				}
			}
		}
	}
	switch base := path.Base(u.Path); base {
	case "", ".", "/", "download", "view":
		return ""
	default:
		return base
	}
}
