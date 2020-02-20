// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcutil

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// GetClientConn returns a gRPC client connection.
func GetClientConn(addr string, caPath string, certPath string, keyPath string) (*grpc.ClientConn, error) {
	opt := grpc.WithInsecure()
	if len(caPath) != 0 {
		var certificates []tls.Certificate
		if len(certPath) != 0 && len(keyPath) != 0 {
			// Load the client certificates from disk
			certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", err)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		creds := credentials.NewTLS(&tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		})

		opt = grpc.WithTransportCredentials(creds)
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cc, err := grpc.Dial(u.Host, opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cc, nil
}
