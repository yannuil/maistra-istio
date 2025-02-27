// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package install

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"istio.io/istio/cni/pkg/config"
	testutils "istio.io/istio/pilot/test/util"
	"istio.io/istio/pkg/file"
)

func TestCheckInstall(t *testing.T) {
	cases := []struct {
		name              string
		expectedFailure   bool
		cniConfigFilename string
		cniConfName       string
		chainedCNIPlugin  bool
		skipInstall       bool
		existingConfFiles map[string]string // {srcFilename: targetFilename, ...}
		cniBinariesPrefix string
	}{
		{
			name:              "preempted config",
			expectedFailure:   true,
			cniConfigFilename: "list.conflist",
			chainedCNIPlugin:  true,
			existingConfFiles: map[string]string{"bridge.conf": "bridge.conf", "list.conflist.golden": "list.conflist"},
		},
		{
			name:              "intentional preempted config invalid",
			expectedFailure:   true,
			cniConfigFilename: "invalid-arr.conflist",
			cniConfName:       "invalid-arr.conflist",
			chainedCNIPlugin:  true,
			existingConfFiles: map[string]string{"bridge.conf": "bridge.conf", "invalid-arr.conflist": "invalid-arr.conflist"},
		},
		{
			name:              "intentional preempted config",
			cniConfigFilename: "list.conflist",
			cniConfName:       "list.conflist",
			chainedCNIPlugin:  true,
			existingConfFiles: map[string]string{"bridge.conf": "bridge.conf", "list.conflist.golden": "list.conflist"},
		},
		{
			name:              "CNI config file removed",
			expectedFailure:   true,
			cniConfigFilename: "file-removed.conflist",
		},
		{
			name:              "CNI config file non-existent but install skipped",
			expectedFailure:   false,
			skipInstall:       true,
			cniConfigFilename: "file-removed.conflist",
		},
		{
			name:              "istio-cni config removed from CNI config file",
			expectedFailure:   true,
			cniConfigFilename: "list.conflist",
			chainedCNIPlugin:  true,
			existingConfFiles: map[string]string{"list.conflist": "list.conflist"},
		},
		{
			name:              "chained CNI plugin",
			cniConfigFilename: "list.conflist",
			chainedCNIPlugin:  true,
			existingConfFiles: map[string]string{"list.conflist.golden": "list.conflist"},
		},
		{
			name:              "standalone CNI plugin istio-cni config not in CNI config file",
			expectedFailure:   true,
			cniConfigFilename: "bridge.conf",
			existingConfFiles: map[string]string{"bridge.conf": "bridge.conf"},
		},
		{
			name:              "standalone CNI plugin",
			cniConfigFilename: "istio-cni.conf",
			existingConfFiles: map[string]string{"istio-cni.conf": "istio-cni.conf"},
		},
		{
			name:              "custom binaries prefix",
			cniConfigFilename: "istio-cni.conf",
			cniBinariesPrefix: "prefix-",
			existingConfFiles: map[string]string{"istio-cni-prefixed.conf": "istio-cni.conf"},
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Create temp directory for files
			tempDir, err := os.MkdirTemp("", fmt.Sprintf("test-case-%d-", i))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.RemoveAll(tempDir); err != nil {
					t.Fatal(err)
				}
			}()

			// Create existing config files if specified in test case
			for srcFilename, targetFilename := range c.existingConfFiles {
				if err := file.AtomicCopy(filepath.Join("testdata", srcFilename), tempDir, targetFilename); err != nil {
					t.Fatal(err)
				}
			}

			cfg := &config.InstallConfig{
				MountedCNINetDir:  tempDir,
				CNIConfName:       c.cniConfName,
				ChainedCNIPlugin:  c.chainedCNIPlugin,
				CNIEnableInstall:  !c.skipInstall,
				CNIBinariesPrefix: c.cniBinariesPrefix,
			}
			err = checkInstall(cfg, filepath.Join(tempDir, c.cniConfigFilename))
			if (c.expectedFailure && err == nil) || (!c.expectedFailure && err != nil) {
				t.Fatalf("expected failure: %t, got %v", c.expectedFailure, err)
			}
		})
	}
}

func TestSleepCheckInstall(t *testing.T) {
	cases := []struct {
		name                  string
		chainedCNIPlugin      bool
		cniConfigFilename     string
		invalidConfigFilename string
		validConfigFilename   string
	}{
		{
			name:                  "chained CNI plugin",
			chainedCNIPlugin:      true,
			cniConfigFilename:     "plugins.conflist",
			invalidConfigFilename: "list.conflist",
			validConfigFilename:   "list.conflist.golden",
		},
		{
			name:                "standalone CNI plugin",
			cniConfigFilename:   "istio-cni.conf",
			validConfigFilename: "istio-cni.conf",
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Create temp directory for files
			tempDir, err := os.MkdirTemp("", fmt.Sprintf("test-case-%d-", i))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.RemoveAll(tempDir); err != nil {
					t.Fatal(err)
				}
			}()

			// Initialize parameters
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := &config.InstallConfig{
				MountedCNINetDir: tempDir,
				ChainedCNIPlugin: c.chainedCNIPlugin,
				CNIEnableInstall: true,
			}
			cniConfigFilepath := filepath.Join(tempDir, c.cniConfigFilename)
			isReady := &atomic.Value{}
			SetNotReady(isReady)

			if len(c.invalidConfigFilename) > 0 {
				// Copy an invalid config file into tempDir
				if err = file.AtomicCopy(filepath.Join("testdata", c.invalidConfigFilename), tempDir, c.cniConfigFilename); err != nil {
					t.Fatal(err)
				}
			}

			t.Log("Expecting an invalid configuration log:")
			if err = sleepCheckInstall(ctx, cfg, cniConfigFilepath, isReady); err != nil {
				t.Fatalf("error should be nil due to invalid config, got: %v", err)
			}
			assert.Falsef(t, isReady.Load().(bool), "isReady should still be false")

			if len(c.invalidConfigFilename) > 0 {
				if err = os.Remove(cniConfigFilepath); err != nil {
					t.Fatal(err)
				}
			}

			// Copy a valid config file into tempDir
			if err = file.AtomicCopy(filepath.Join("testdata", c.validConfigFilename), tempDir, c.cniConfigFilename); err != nil {
				t.Fatal(err)
			}

			// Listen for isReady to be set to true
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			readyChan := make(chan bool)
			go func(ctx context.Context, tick <-chan time.Time) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick:
						if isReady.Load().(bool) {
							readyChan <- true
						}
					}
				}
			}(ctx, ticker.C)

			// Listen to sleepCheckInstall return value
			// Should detect a valid configuration and wait indefinitely for a file modification
			errChan := make(chan error)
			go func(ctx context.Context, cfg *config.InstallConfig, cniConfigFilepath string, isReady *atomic.Value) {
				errChan <- sleepCheckInstall(ctx, cfg, cniConfigFilepath, isReady)
			}(ctx, cfg, cniConfigFilepath, isReady)

			select {
			case <-readyChan:
				assert.Truef(t, isReady.Load().(bool), "isReady should have been set to true")
			case err = <-errChan:
				if err == nil {
					t.Fatal("invalid configuration detected")
				}
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for isReady to be set to true")
			}

			// Remove Istio CNI's config
			t.Log("Expecting an invalid configuration log:")
			if len(c.invalidConfigFilename) > 0 {
				if err = file.AtomicCopy(filepath.Join("testdata", c.invalidConfigFilename), tempDir, c.cniConfigFilename); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = os.Remove(cniConfigFilepath); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case err = <-errChan:
				if err != nil {
					// An invalid configuration should return nil
					// Either an invalid config did not return nil (which is an issue) or an unexpected error occurred
					t.Fatal(err)
				}
				assert.Falsef(t, isReady.Load().(bool), "isReady should have been set to false after returning from sleepCheckInstall")
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for invalid configuration to be detected")
			}
		})
	}
}

func TestCleanup(t *testing.T) {
	cases := []struct {
		name                   string
		expectedFailure        bool
		chainedCNIPlugin       bool
		configFilename         string
		existingConfigFilename string
		expectedConfigFilename string
		cniBinariesPrefix      string
	}{
		{
			name:                   "chained CNI plugin",
			chainedCNIPlugin:       true,
			configFilename:         "list.conflist",
			existingConfigFilename: "list-with-istio.conflist",
			expectedConfigFilename: "list-no-istio.conflist",
		},
		{
			name:                   "standalone CNI plugin",
			configFilename:         "istio-cni.conf",
			existingConfigFilename: "istio-cni.conf",
		},
		{
			name:                   "prefix",
			cniBinariesPrefix:      "prefix-",
			configFilename:         "list-cni-prefixed.conf",
			existingConfigFilename: "list-with-istio.conflist",
			expectedConfigFilename: "list-no-istio.conflist",
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Create temp directory for files
			cniNetDir, err := os.MkdirTemp("", fmt.Sprintf("test-case-%d-cni-net", i))
			if err != nil {
				t.Fatal(err)
			}
			cniBinDir, err := os.MkdirTemp("", fmt.Sprintf("test-case-%d-cni-bin", i))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err1 := os.RemoveAll(cniNetDir)
				err2 := os.RemoveAll(cniBinDir)
				if err1 != nil {
					t.Fatal(err1)
				} else if err2 != nil {
					t.Fatal(err1)
				}
			}()

			// Create existing config file if specified in test case
			cniConfigFilePath := filepath.Join(cniNetDir, c.configFilename)
			if err := file.AtomicCopy(filepath.Join("testdata", c.existingConfigFilename), cniNetDir, c.configFilename); err != nil {
				t.Fatal(err)
			}

			// Create existing binary files
			filename := c.cniBinariesPrefix + "istio-cni"
			if err := os.WriteFile(filepath.Join(cniBinDir, filename), []byte{1, 2, 3}, 0o755); err != nil {
				t.Fatal(err)
			}

			// Create kubeconfig
			kubeConfigFilePath := filepath.Join(cniNetDir, "kubeconfig")
			if err := os.WriteFile(kubeConfigFilePath, []byte{1, 2, 3}, 0o755); err != nil {
				t.Fatal(err)
			}

			cfg := &config.InstallConfig{
				MountedCNINetDir:  cniNetDir,
				ChainedCNIPlugin:  c.chainedCNIPlugin,
				CNIBinariesPrefix: c.cniBinariesPrefix,
				CNIBinTargetDirs:  []string{cniBinDir},
			}

			isReady := &atomic.Value{}
			isReady.Store(false)
			installer := NewInstaller(cfg, isReady)
			installer.cniConfigFilepath = cniConfigFilePath
			installer.kubeconfigFilepath = kubeConfigFilePath
			err = installer.Cleanup()
			if (c.expectedFailure && err == nil) || (!c.expectedFailure && err != nil) {
				t.Fatalf("expected failure: %t, got %v", c.expectedFailure, err)
			}

			// check if conf file is deleted/conflist file is updated
			if c.chainedCNIPlugin {
				resultConfig := testutils.ReadFile(cniConfigFilePath, t)

				goldenFilepath := filepath.Join("testdata", c.expectedConfigFilename)
				goldenConfig := testutils.ReadFile(goldenFilepath, t)
				testutils.CompareBytes(resultConfig, goldenConfig, goldenFilepath, t)
			} else if file.Exists(cniConfigFilePath) {
				t.Fatalf("file %s was not deleted", c.configFilename)
			}

			// check if kubeconfig is deleted
			if file.Exists(kubeConfigFilePath) {
				t.Fatal("kubeconfig was not deleted")
			}

			// check if binaries are deleted
			if file.Exists(filepath.Join(cniBinDir, "istio-cni")) {
				t.Fatalf("File %s was not deleted", "istio-cni")
			}
		})
	}
}
