package coretls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// core is an HTTPS server with a certificate no system trusts (httptest's
// own, for 127.0.0.1), as a self-signed core is.
func core(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	t.Cleanup(srv.Close)
	return srv
}

// selfSigned is a self-signed certificate for 127.0.0.1, in DER.
func selfSigned(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "not core"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func get(t *testing.T, c Config, url string) error {
	t.Helper()
	hc, err := c.HTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get(url)
	if err == nil {
		_ = resp.Body.Close()
	}
	return err
}

func TestTrust(t *testing.T) {
	srv := core(t)
	fp := Fingerprint(srv.Certificate().Raw)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "core.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	// Another certificate, for the same address: not core's.
	otherDER := selfSigned(t)
	other := filepath.Join(dir, "other.pem")
	_ = os.WriteFile(other, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: otherDER}), 0o600)
	wrongFP := Fingerprint(otherDER)

	for name, tc := range map[string]struct {
		env  map[string]string
		mode string
		ok   bool
	}{
		"the system's":           {nil, "system", false},
		"core's PEM":             {map[string]string{EnvCAFile: caFile}, "ca-file", true},
		"another PEM":            {map[string]string{EnvCAFile: other}, "ca-file", false},
		"its fingerprint":        {map[string]string{EnvCertSHA256: fp}, "fingerprint", true},
		"its fingerprint, plain": {map[string]string{EnvCertSHA256: strings.ToLower(strings.ReplaceAll(fp, ":", ""))}, "fingerprint", true},
		"another fingerprint":    {map[string]string{EnvCertSHA256: wrongFP}, "fingerprint", false},
		"insecure":               {map[string]string{EnvInsecure: "true"}, "insecure", true},
		"insecure off":           {map[string]string{EnvInsecure: "false"}, "system", false},
		// The first set decides: insecure, then the fingerprint, then the file.
		"insecure first":       {map[string]string{EnvInsecure: "1", EnvCertSHA256: wrongFP}, "insecure", true},
		"fingerprint over CAs": {map[string]string{EnvCertSHA256: fp, EnvCAFile: other}, "fingerprint", true},
	} {
		c, err := FromEnv(env(tc.env))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if c.Mode() != tc.mode {
			t.Errorf("%s: mode %s, want %s", name, c.Mode(), tc.mode)
		}
		err = get(t, c, srv.URL)
		if (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
		if name == "another fingerprint" && (err == nil || !strings.Contains(err.Error(), fp)) {
			t.Errorf("a wrong fingerprint does not say core's: %v", err)
		}
	}
}

func TestFromEnvRefusals(t *testing.T) {
	for name, m := range map[string]map[string]string{
		"not a boolean":     {EnvInsecure: "yes please"},
		"short fingerprint": {EnvCertSHA256: "AB:CD"},
		"not hex":           {EnvCertSHA256: strings.Repeat("zz", 32)},
	} {
		if _, err := FromEnv(env(m)); err == nil {
			t.Errorf("%s: read", name)
		}
	}
	// A CA file that does not read, or holds no certificate, fails as the
	// client is made.
	for name, path := range map[string]string{"missing": filepath.Join(t.TempDir(), "none.pem"), "not PEM": writeFile(t, "hello")} {
		if _, err := (Config{CAFile: path}).HTTPClient(); err == nil || !strings.Contains(err.Error(), EnvCAFile) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func writeFile(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFingerprint(t *testing.T) {
	fp := Fingerprint([]byte("x"))
	if len(fp) != 95 || strings.ToUpper(fp) != fp {
		t.Fatalf("fingerprint %q", fp)
	}
	b, err := ParseFingerprint("sha256/" + strings.ReplaceAll(fp, ":", " "))
	if err != nil || Fingerprint([]byte("x")) != fp || len(b) != 32 {
		t.Fatalf("parse: %v", err)
	}
}
