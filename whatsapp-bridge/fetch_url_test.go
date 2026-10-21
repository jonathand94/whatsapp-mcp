package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// allowLoopback is the test-only address policy.
//
// It permits loopback, which is the only place an httptest server can live, and
// defers to blockedIP for everything else. That second half is the point: a
// test that expects a refusal is still being judged by the real guard, not by a
// stub that happens to agree with it.
func allowLoopback(addr netip.Addr) (bool, string) {
	if addr.Unmap().IsLoopback() {
		return false, ""
	}
	return blockedIP(addr)
}

// testFetcher can reach a local test server and nothing else it would not
// otherwise reach. The redirect hook and the dialer guard are the production
// ones, wired to a policy that differs from the real one on loopback alone.
func testFetcher(t *testing.T, srv *httptest.Server) fetcher {
	t.Helper()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.DialContext = guardedDialContext(allowLoopback)
	return fetcher{
		policy: allowLoopback,
		client: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: redirectChecker(allowLoopback),
			Transport:     tr,
		},
	}
}

// fetchToMemory runs fetchRemoteFileTo into a buffer. Downloads stream to disk
// now; these tests were written against the in-memory shape, and what they
// check — the guard, the HTML refusal, the caps, the naming — is the same
// either way.
func fetchToMemory(ctx context.Context, rawURL string, maxBytes int64, f fetcher) ([]byte, string, error) {
	var buf bytes.Buffer
	name, _, err := fetchRemoteFileTo(ctx, rawURL, maxBytes, f, &buf, nil)
	if err != nil {
		return nil, "", err
	}
	return buf.Bytes(), name, nil
}

// --- the redirect guard -----------------------------------------------------

// TestRedirectToInstanceMetadataIsBlockedAtTheHop is the test this feature
// exists to keep passing.
//
// A URL that validates cleanly can still answer 302 with
// 169.254.169.254/latest/meta-data/iam/security-credentials/, and the body of
// that response is a set of cloud credentials on its way into a WhatsApp
// message. Nothing about the request looks wrong until the second hop, so
// anyone "simplifying" the redirect handling into following redirects
// automatically breaks exactly this and nothing else, silently.
func TestRedirectToInstanceMetadataIsBlockedAtTheHop(t *testing.T) {
	targets := map[string]string{
		"aws/gcp/azure metadata": "https://169.254.169.254/latest/meta-data/iam/security-credentials/",
		"metadata in v6 clothes": "https://[::ffff:169.254.169.254]/latest/meta-data/",
		"link-local v6":          "https://[fe80::1]/latest/meta-data/",
		"private range":          "https://10.0.0.5/admin",
	}
	for name, target := range targets {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusFound)
			}))
			defer srv.Close()

			_, _, err := fetchToMemory(t.Context(), srv.URL+"/photo.png", 1<<20, testFetcher(t, srv))
			if err == nil {
				t.Fatal("a redirect to an internal address must not be followed")
			}
			if !errors.Is(err, errBlockedTarget) {
				t.Fatalf("the address guard should have refused this, got %v", err)
			}
		})
	}
}

// TestRedirectCheckerRefusesInternalLiterals exercises the hook under the real
// policy, which the download tests cannot: they have to serve from loopback,
// the very range this has to refuse.
func TestRedirectCheckerRefusesInternalLiterals(t *testing.T) {
	check := redirectChecker(nil) // nil → the production policy

	refused := []string{
		"https://127.0.0.1:8080/x",
		"https://[::1]:8080/x",
		"https://169.254.169.254/latest/meta-data/",
		"https://[::ffff:127.0.0.1]/x",
		"https://10.0.0.5/x",
		"https://172.16.0.1/x",
		"https://172.31.255.254/x",
		"https://192.168.1.1/x",
		"https://100.64.0.1/x", // CGNAT
		"https://0.0.0.0/x",
		"http://example.com/x", // downgraded scheme, not an address
		"file:///etc/passwd",
	}
	for _, raw := range refused {
		t.Run(raw, func(t *testing.T) {
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			err = check(&http.Request{URL: u}, nil)
			if err == nil {
				t.Fatal("expected this hop to be refused")
			}
			if !errors.Is(err, errBlockedTarget) {
				t.Fatalf("refusal should come from the guard, got %v", err)
			}
		})
	}

	// A public https hop is the case that must keep working, or a normalized
	// Drive link never reaches the file.
	u, err := url.Parse("https://drive.usercontent.google.com/download?id=abc")
	if err != nil {
		t.Fatal(err)
	}
	if err := check(&http.Request{URL: u}, nil); err != nil {
		t.Fatalf("a public https hop should be allowed: %v", err)
	}
}

func TestRedirectLimit(t *testing.T) {
	var srv *httptest.Server
	hops := 0
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops++
		http.Redirect(w, r, fmt.Sprintf("%s/hop%d", srv.URL, hops), http.StatusFound)
	}))
	defer srv.Close()

	_, _, err := fetchToMemory(t.Context(), srv.URL+"/start", 1<<20, testFetcher(t, srv))
	if err == nil {
		t.Fatal("an endless redirect chain must be refused")
	}
	if !errors.Is(err, errBlockedTarget) {
		t.Fatalf("the cap should report as a guard refusal, got %v", err)
	}
	// The cap has to bind, not merely be declared: without it this handler
	// would answer forever.
	if hops > maxFetchRedirects+1 {
		t.Fatalf("followed %d hops with a cap of %d", hops, maxFetchRedirects)
	}
}

// --- the address guard ------------------------------------------------------

func TestBlockedIPRanges(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.1.2.3", "::1",
		"169.254.169.254",        // instance metadata on AWS, GCP and Azure
		"::ffff:169.254.169.254", // the same address, v4-mapped
		"169.254.0.1", "fe80::1",
		"10.0.0.1", "10.255.255.254",
		"172.16.0.1", "172.31.255.254",
		"192.168.0.1", "192.168.255.254",
		"100.64.0.1", "100.127.255.254", // CGNAT
		"fc00::1", "fd00:ec2::254", // ULA, including AWS's IPv6 metadata address
		"0.0.0.0", "::",
		"224.0.0.1", "ff02::1", // multicast
	}
	for _, s := range blocked {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		bad, why := blockedIP(addr)
		if !bad {
			t.Fatalf("%s must be blocked", s)
		}
		if why == "" {
			t.Fatalf("%s was blocked without a reason to report", s)
		}
	}

	// The boundaries matter as much as the ranges: a guard that blocks the
	// whole internet is discovered immediately, one that is a bit too narrow is
	// not discovered at all.
	allowed := []string{
		"8.8.8.8", "1.1.1.1", "142.250.185.78",
		"9.255.255.255",                // just below 10/8
		"11.0.0.1",                     // just above 10/8
		"172.15.255.254", "172.32.0.1", // either side of 172.16/12
		"192.167.255.254", "192.169.0.1", // either side of 192.168/16
		"100.63.255.254", "100.128.0.1", // either side of 100.64/10
		"169.253.255.254", "169.255.0.1", // either side of 169.254/16
		"2606:4700::1111",
	}
	for _, s := range allowed {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if bad, why := blockedIP(addr); bad {
			t.Fatalf("%s should be reachable, blocked as %q", s, why)
		}
	}
}

func TestValidateFetchURLRequiresHTTPS(t *testing.T) {
	// An http hop can be rewritten in flight by anything on the path, which
	// would undo every check made before it.
	for _, raw := range []string{
		"http://example.com/a.png",
		"ftp://example.com/a.png",
		"gopher://example.com/",
		"file:///C:/Users/jonat/.ssh/id_rsa",
		"https:///no-host.png",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if err := validateFetchURL(u, nil); err == nil {
			t.Fatalf("%s should be rejected", raw)
		} else if !errors.Is(err, errBlockedTarget) {
			t.Fatalf("%s: refusal should come from the guard, got %v", raw, err)
		}
	}
}

func TestGuardedDialRefusesNamesThatResolveInward(t *testing.T) {
	dial := guardedDialContext(nil) // the production policy

	// "localhost" is not an address literal, so the URL check cannot judge it.
	// This is the layer that has to, and it is also the layer that catches a
	// public name whose DNS answer points inward.
	if _, err := dial(t.Context(), "tcp", "localhost:8080"); err == nil {
		t.Fatal("localhost must not be dialable")
	} else if !errors.Is(err, errBlockedTarget) {
		t.Fatalf("expected a guard refusal, got %v", err)
	}
}

// --- Drive links ------------------------------------------------------------

func TestNormalizeDriveURL(t *testing.T) {
	const id = "1vlfw7ItHujtl5F2XuHHImg-v25XycGpD"
	direct := "https://drive.usercontent.google.com/download?id=" + id + "&export=download&confirm=t"

	tests := []struct {
		name, in, want string
	}{
		// The shape a user actually copies out of the Drive UI. It serves the
		// web application, so downloading it verbatim yields HTML.
		{"share link", "https://drive.google.com/file/d/" + id + "/view?usp=sharing", direct},
		{"share link, no query", "https://drive.google.com/file/d/" + id + "/view", direct},
		{"open by id", "https://drive.google.com/open?id=" + id, direct},
		// /uc serves the file but interposes an antivirus warning page once the
		// file is more than a few megabytes.
		{"uc download", "https://drive.google.com/uc?export=download&id=" + id, direct},
		{"docs host", "https://docs.google.com/uc?id=" + id, direct},
		{"already direct", direct, direct},
		{"not drive", "https://example.com/file/d/" + id + "/view", "https://example.com/file/d/" + id + "/view"},
		{"drive with no id", "https://drive.google.com/drive/my-drive", "https://drive.google.com/drive/my-drive"},
		{"unparseable is passed through", "https://drive.google.com/%zz", "https://drive.google.com/%zz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeDriveURL(tc.in); got != tc.want {
				t.Fatalf("normalizeDriveURL(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

// --- downloading ------------------------------------------------------------

func TestFetchRemoteFileDownloads(t *testing.T) {
	// 2 MB is the size that made file_base64 unworkable: ~2.8 MB once encoded,
	// which is more than a conversation can carry.
	body := make([]byte, 2<<20)
	copy(body, tinyPNG)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ua := r.Header.Get("User-Agent"); ua == "" {
			t.Error("the request should identify itself; some hosts serve nothing otherwise")
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Disposition", `attachment; filename="recibo.png"`)
		w.Write(body)
	}))
	defer srv.Close()

	data, name, err := fetchToMemory(t.Context(), srv.URL+"/download", maxDocumentBytes, testFetcher(t, srv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != len(body) {
		t.Fatalf("got %d bytes, want %d", len(data), len(body))
	}
	// The name has to survive the download, or a document reaches the recipient
	// named after the draft id.
	if name != "recibo.png" {
		t.Fatalf("name hint = %q, want recibo.png", name)
	}
}

func TestFetchRemoteFileRejectsHTML(t *testing.T) {
	// Drive answers 200 OK with a login page for a file that is not public.
	// Without this check the recipient receives that page dressed up as a
	// photo, and nothing in the flow ever reported a problem.
	const loginPage = "<!DOCTYPE html><html><head><title>Iniciar sesion</title></head><body>...</body></html>"

	t.Run("declared as html", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte(loginPage))
		}))
		defer srv.Close()

		_, _, err := fetchToMemory(t.Context(), srv.URL+"/file", 1<<20, testFetcher(t, srv))
		if !errors.Is(err, errHTMLBody) {
			t.Fatalf("an HTML body must be refused, got %v", err)
		}
		// The message has to say what to do about it, not just what happened.
		if !strings.Contains(err.Error(), "cualquier persona con el enlace") {
			t.Fatalf("the error should tell the user how to share the file: %v", err)
		}
	})

	t.Run("html with the content type suppressed", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header()["Content-Type"] = nil // omit it entirely
			w.Write([]byte(loginPage))
		}))
		defer srv.Close()

		// Sniffing the body is what keeps the check from being sidestepped by
		// simply not declaring a type.
		_, _, err := fetchToMemory(t.Context(), srv.URL+"/file", 1<<20, testFetcher(t, srv))
		if !errors.Is(err, errHTMLBody) {
			t.Fatalf("an undeclared HTML body must still be refused, got %v", err)
		}
	})
}

func TestFetchRemoteFileCapsStreamWhenLengthIsUnknown(t *testing.T) {
	const ceiling = 4 << 10

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.WriteHeader(http.StatusOK)
		// Flushing before the body forces chunked encoding, so the response
		// carries no Content-Length — the case a header-only check misses.
		w.(http.Flusher).Flush()
		w.Write(make([]byte, ceiling*4))
	}))
	defer srv.Close()

	_, _, err := fetchToMemory(t.Context(), srv.URL+"/big.pdf", ceiling, testFetcher(t, srv))
	if err == nil {
		t.Fatal("a body over the ceiling must be refused even with no Content-Length")
	}
	// "supera" is the mid-stream refusal; "declara" would mean the header
	// pre-check fired, which cannot happen here and would not prove anything.
	if !strings.Contains(err.Error(), "supera") {
		t.Fatalf("the limit should have bound during the read: %v", err)
	}
}

func TestFetchRemoteFileRejectsDeclaredOversize(t *testing.T) {
	// Small enough that Go buffers the whole response and sets Content-Length,
	// which exercises the cheap pre-check instead of the streaming one.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.Write(make([]byte, 2<<10))
	}))
	defer srv.Close()

	_, _, err := fetchToMemory(t.Context(), srv.URL+"/big.pdf", 1<<10, testFetcher(t, srv))
	if err == nil {
		t.Fatal("a declared oversize body must be refused")
	}
	if !strings.Contains(err.Error(), "declara") {
		t.Fatalf("expected the Content-Length pre-check to fire: %v", err)
	}
}

func TestFetchRemoteFileRejectsBadStatusAndEmptyBody(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusNotFound)
		}))
		defer srv.Close()

		if _, _, err := fetchToMemory(t.Context(), srv.URL+"/x", 1<<20, testFetcher(t, srv)); err == nil {
			t.Fatal("a 404 must not become a draft")
		}
	})

	t.Run("empty body", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/pdf")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		// An empty file uploads "successfully" and arrives as an unopenable
		// attachment, same as the file_path case.
		if _, _, err := fetchToMemory(t.Context(), srv.URL+"/x", 1<<20, testFetcher(t, srv)); err == nil {
			t.Fatal("an empty download must be refused")
		}
	})
}

func TestFetchNameHint(t *testing.T) {
	hint := func(cd, rawURL string) string {
		t.Helper()
		u, err := url.Parse(rawURL)
		if err != nil {
			t.Fatal(err)
		}
		h := http.Header{}
		if cd != "" {
			h.Set("Content-Disposition", cd)
		}
		return fetchNameHint(&http.Response{Header: h}, u)
	}

	if got := hint(`attachment; filename="contrato.pdf"`, "https://drive.usercontent.google.com/download?id=x"); got != "contrato.pdf" {
		t.Fatalf("Content-Disposition should win, got %q", got)
	}
	// A server-supplied name is attacker-influenced text that goes on to choose
	// a file extension, so path separators must not survive it.
	if got := hint(`attachment; filename="../../etc/passwd"`, "https://x.test/download"); got != "passwd" {
		t.Fatalf("traversal should be stripped, got %q", got)
	}
	if got := hint("", "https://x.test/files/foto.jpg"); got != "foto.jpg" {
		t.Fatalf("the URL path is the fallback, got %q", got)
	}
	// "download" and "view" are endpoint names, not file names; returning them
	// would put a nonsense extension on the stored file.
	for _, raw := range []string{
		"https://drive.usercontent.google.com/download",
		"https://drive.google.com/file/d/abc/view",
		"https://x.test/",
	} {
		if got := hint("", raw); got != "" {
			t.Fatalf("%s should yield no hint, got %q", raw, got)
		}
	}
}

// --- wiring into the draft flow ---------------------------------------------

func TestMaterializeRefusesBlockedURL(t *testing.T) {
	cfg := testConfig(t)

	// The draft path uses the zero fetcher, so it must inherit the guard rather
	// than have its own opinion about addresses.
	for _, raw := range []string{
		"https://169.254.169.254/latest/meta-data/iam/security-credentials/",
		"https://127.0.0.1:8090/api/sends",
		"http://example.com/foto.png",
		"file:///C:/Users/jonat/.claude/whatsapp-mcp-remote/state/token.json",
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := materializeOutboundFile(t.Context(), cfg, "d", createDraftRequest{
				SendType: "file",
				FileURL:  raw,
			})
			if err == nil {
				t.Fatal("expected the draft to be refused")
			}
			if !errors.Is(err, errBlockedTarget) {
				t.Fatalf("refusal should come from the address guard, got %v", err)
			}
		})
	}

	// A refused draft must leave nothing behind: the expiry sweep only knows
	// about files a row points at.
	if entries, err := os.ReadDir(outboundDir(cfg)); err == nil && len(entries) != 0 {
		t.Fatalf("a refused draft wrote %d file(s) to disk", len(entries))
	}
}
