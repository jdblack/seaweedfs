package dash

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Fork-local addition (not present in upstream SeaweedFS). The dashboard's view of
// the cluster EC ratio policy (ec.config) and the filer plumbing behind it. The
// admin is one of the two writers of the policy document — the other is the
// `ec.config` shell command — and the place an operator reads it from. The volume
// server never reads this file: the ratio reaches it on the generate request.

// ECConfigForkEntry is one (data, parity) pair, for display and for the API.
type ECConfigForkEntry struct {
	Collection   string `json:"collection,omitempty"`
	DataShards   int    `json:"data_shards"`
	ParityShards int    `json:"parity_shards"`
}

// ECConfigForkView is everything the EC Configuration page renders.
type ECConfigForkView struct {
	Global            *ECConfigForkEntry  `json:"global,omitempty"`
	Collections       []ECConfigForkEntry `json:"collections"`
	BuildDataShards   int                 `json:"build_data_shards"`
	BuildParityShards int                 `json:"build_parity_shards"`
	FilerAvailable    bool                `json:"filer_available"`
	FilerAddress      string              `json:"filer_address,omitempty"`
}

// ECConfigView reads the policy this process currently holds.
func (s *AdminServer) ECConfigView() ECConfigForkView {
	view := ECConfigForkView{
		Collections:       []ECConfigForkEntry{},
		BuildDataShards:   erasure_coding.DataShardsCount,
		BuildParityShards: erasure_coding.ParityShardsCount,
	}
	if global, found := erasure_coding.GlobalECConfig(); found {
		view.Global = &ECConfigForkEntry{DataShards: global.DataShards, ParityShards: global.ParityShards}
	}
	for _, override := range erasure_coding.CollectionECConfigs() {
		view.Collections = append(view.Collections, ECConfigForkEntry{
			Collection:   override.Collection,
			DataShards:   override.DataShards,
			ParityShards: override.ParityShards,
		})
	}
	if filer, ok := s.ecPolicyFiler(); ok {
		view.FilerAvailable = true
		view.FilerAddress = string(filer)
	}
	return view
}

// ecPolicyFiler returns a filer to read or write the policy document on, using
// the same discovery the rest of the dashboard relies on.
func (s *AdminServer) ecPolicyFiler() (pb.ServerAddress, bool) {
	for _, filer := range s.getDiscoveredFilers() {
		if strings.TrimSpace(filer) != "" {
			return pb.ServerAddress(filer), true
		}
	}
	return "", false
}

// ReloadECPolicyFromFiler installs the persisted policy in this process, so the
// admin's own decisions (EC job parameters, the page) match what the shell would
// use. Called when the page is opened and after every save.
func (s *AdminServer) ReloadECPolicyFromFiler() error {
	filer, ok := s.ecPolicyFiler()
	if !ok {
		return fmt.Errorf("no filer discovered: cannot read the EC policy")
	}
	return ec.LoadECConfigFromFiler(s.grpcDialOption, filer)
}

// SaveECPolicyToFiler persists the policy this process holds.
func (s *AdminServer) SaveECPolicyToFiler() error {
	filer, ok := s.ecPolicyFiler()
	if !ok {
		return fmt.Errorf("no filer discovered: cannot save the EC policy")
	}
	return ec.SaveECConfigToFiler(s.grpcDialOption, filer)
}
