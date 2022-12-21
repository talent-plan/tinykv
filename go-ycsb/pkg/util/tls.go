package util

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// CreateTLSConfig creats a TLS configuration.
func CreateTLSConfig(caPath, certPath, keyPath string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecureSkipVerify,
		Renegotiation:      tls.RenegotiateNever,
	}

	if caPath != "" {
		pool, err := makeCertPool([]string{caPath})
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}

	if certPath != "" && keyPath != "" {
		err := loadCertificate(tlsConfig, certPath, keyPath)
		if err != nil {
			return nil, err
		}
	}

	return tlsConfig, nil
}

func makeCertPool(certFiles []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, certFile := range certFiles {
		pem, err := ioutil.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("could not read certificate %q: %v", certFile, err)
		}
		ok := pool.AppendCertsFromPEM(pem)
		if !ok {
			return nil, fmt.Errorf("could not parse any PEM certificates %q: %v", certFile, err)
		}
	}
	return pool, nil
}

func loadCertificate(config *tls.Config, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("could not load keypair %s:%s: %v", certFile, keyFile, err)
	}

	config.Certificates = []tls.Certificate{cert}
	config.BuildNameToCertificate()
	return nil
}
