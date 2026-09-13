package ec_vacuum

import "github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"

// The EC vacuum core (transport, local compaction, candidate enumeration) lives
// in weed/ec/ecvacuum so that this worker and the `ec.vacuum` shell command can
// share one implementation. It cannot live in this package: this package imports
// weed/plugin/worker, which imports weed/shell, so weed/shell importing this
// package would be an import cycle. weed/ec/ecvacuum depends on none of those.
//
// These aliases keep the worker's existing call sites and its tests unchanged, so
// the extraction did not have to touch the test suite. Only the symbols the
// worker (or its tests) still reference are aliased.
type (
	vacuumTarget     = ecvacuum.VacuumTarget
	vacuumOptions    = ecvacuum.Options
	vacuumResult     = ecvacuum.Result
	shardTransport   = ecvacuum.ShardTransport
	garbageCandidate = ecvacuum.Candidate
)

var (
	newClusterTransport        = ecvacuum.NewClusterTransport
	vacuumLocalDir             = ecvacuum.VacuumLocalDir
	loadVacuumOptions          = ecvacuum.LoadVacuumOptions
	buildVacuumTarget          = ecvacuum.BuildVacuumTarget
	hasAllDataShards           = ecvacuum.HasAllDataShards
	shardBaseName              = ecvacuum.ShardBaseName
	enumerateGarbageEcVolumes  = ecvacuum.EnumerateGarbageEcVolumes
	filterCandidatesByDiskType = ecvacuum.FilterCandidatesByDiskType
	verifyEncodedShards        = ecvacuum.VerifyEncodedShards
)
