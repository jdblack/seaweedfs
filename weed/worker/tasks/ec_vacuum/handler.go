package ec_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	storagetypes "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  jobType,
		Category: pluginworker.CategoryHeavy,
		Aliases:  []string{"ec-vacuum", "ec.vacuum"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewEcVacuumHandler(opts.GrpcDialOption, opts.WorkingDir), nil
		},
	})
}

// VacuumHandler is the plugin job handler for EC volume vacuum.
type VacuumHandler struct {
	grpcDialOption grpc.DialOption
	workingDir     string

	// fetchTopology, decodeVolume and transport are seams so unit tests can drive
	// detection and both actions without a live cluster. fetchTopology also
	// returns the master's volume size limit, which is what makes an EC volume's
	// fullness (and so the decode threshold) computable.
	fetchTopology func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, uint64, error)
	decodeVolume  func(ctx context.Context, masters []string, topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) error
	transport     shardTransport
}

// NewEcVacuumHandler creates the EC vacuum handler. workingDir is the worker's
// -workingDir, where a volume's shards are collected and compacted.
func NewEcVacuumHandler(dialOpt grpc.DialOption, workingDir string) *VacuumHandler {
	h := &VacuumHandler{
		grpcDialOption: dialOpt,
		workingDir:     workingDir,
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, uint64, error) {
			return fetchTopologyFromMasters(ctx, masters, dialOpt)
		},
		transport: newClusterTransport(dialOpt),
	}
	h.decodeVolume = h.decodeEcVolume
	return h
}

// decodeEcVolume decodes one EC volume back to a regular volume through the shared
// ec.DoEcDecode (the same code behind `ec.decode`), so the interrupted-decode
// recovery is not reimplemented here. Like ec_bitrot_scan's repair it supplies an
// Env with IsLocked true: the convention for a worker, which holds no admin lock.
//
// checkMinFreeSpace is false on purpose. DoEcDecode purges an empty volume without
// creating one, so the free-slot pre-check would refuse the very case that needs no
// slot — and an empty volume is the one that frees the most. A target with
// genuinely no room fails the generate RPC, a real reason rather than a preemptive
// one.
func (h *VacuumHandler) decodeEcVolume(ctx context.Context, masters []string, topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) error {
	env := &ec.Env{
		GrpcDialOption: h.grpcDialOption,
		FetchTopology: func(delay time.Duration) (*master_pb.TopologyInfo, uint64, error) {
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				}
			}
			return h.fetchTopology(ctx, masters)
		},
		IsLocked: func() bool { return true },
	}
	dt := storagetypes.ToDiskType(diskType)
	// One volume per job and no free-space gate, so there is no batch accounting
	// for a disk usage state to carry.
	return ec.DoEcDecode(env, topo, collection, needle.VolumeId(volumeID), dt, false, nil)
}

func (h *VacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "EC Vacuum",
		Description:             "Compact deleted needles out of EC volumes, and decode volumes whose live data has shrunk below the configured fullness",
		Weight:                  70,
	}
}

func (h *VacuumHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "EC Vacuum",
		Description:       "Detect EC volumes to compact (high deleted-needle ratio) or to decode back to regular volumes (live data below a fullness threshold).",
		Icon:              "fas fa-broom",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-vacuum-admin",
			Title:       "EC Vacuum Admin Config",
			Description: "Admin-side scope and threshold for EC vacuum detection.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope & Thresholds",
					Description: "Optional filters, plus the thresholds that choose each action.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldCollectionFilter,
							Label:       "Collection Filter",
							Description: "Only vacuum EC volumes in matching collections. Comma-separated names, wildcards, or regex patterns. Empty = all.",
							Placeholder: "all collections",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        fieldDiskType,
							Label:       "Disk Type",
							Description: "Only vacuum EC shards on this disk type (e.g. hdd, ssd). Empty = all.",
							Placeholder: "all disks",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        fieldGarbageThreshold,
							Label:       "Deleted Ratio Threshold",
							Description: "Vacuum a volume once its deleted-needle ratio reaches this fraction (0-1). Default 0.30 = 30%.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
						{
							Name:        fieldDecodeBelowFullness,
							Label:       "Decode Below Fullness",
							Description: "Decode an EC volume back to a regular volume once its live data falls to this fraction of the volume size limit (0-1). 0 disables decoding. Must stay below the erasure_coding job's fullness_ratio (default 0.95), or a decoded volume is immediately re-encoded.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    false,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				fieldCollectionFilter:    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldDiskType:            {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldGarbageThreshold:    {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: defaultGarbageThreshold}},
				fieldDecodeBelowFullness: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: defaultDecodeBelowFullnessPercent}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-vacuum-worker",
			Title:       "EC Vacuum Worker Config",
			Description: "Worker-side detection cadence and per-volume timeout.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "cadence",
					Title:       "Cadence",
					Description: "Controls how frequently detection proposes new vacuums.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldMinIntervalMins,
							Label:       "Minimum Interval (minutes)",
							Description: "Skip detection when the last successful run is more recent than this many minutes (30 = every 30 minutes).",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        fieldScanTimeoutSecs,
							Label:       "Per-Volume Timeout (seconds)",
							Description: "Bound one volume's collect and redistribute stages (3600 = one hour). The local compaction between them is not interruptible, so it can overrun this; keep the admin execution timeout above the worst-case compaction too.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				fieldMinIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinIntervalMinutes}},
				fieldScanTimeoutSecs: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanTimeoutSeconds}},
			},
		},
		// Global concurrency 4 / per-worker 1 mirrors the enterprise defaults;
		// compaction is disk- and CPU-heavy, so it is capped tightly by default.
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalMinutes:      defaultMinIntervalMinutes,
			DetectionTimeoutSeconds:       600,
			MaxJobsPerDetection:           100,
			GlobalExecutionConcurrency:    4,
			PerWorkerExecutionConcurrency: 1,
			RetryLimit:                    1,
			RetryBackoffSeconds:           60,
			JobTypeMaxRuntimeSeconds:      7200,
			ExecutionTimeoutSeconds:       defaultScanTimeoutSeconds,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			fieldMinIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinIntervalMinutes}},
			fieldScanTimeoutSecs: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanTimeoutSeconds}},
		},
	}
}

// fetchTopologyFromMasters returns a topology snapshot from the first reachable
// master.
func fetchTopologyFromMasters(ctx context.Context, masters []string, dialOpt grpc.DialOption) (*master_pb.TopologyInfo, uint64, error) {
	if len(masters) == 0 {
		return nil, 0, fmt.Errorf("no master addresses provided in cluster context")
	}
	var lastErr error
	for _, master := range masters {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		resp, err := pluginworker.FetchVolumeList(ctx, master, dialOpt)
		if err != nil {
			lastErr = err
			continue
		}
		return resp.GetTopologyInfo(), resp.GetVolumeSizeLimitMb(), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no usable master address")
	}
	return nil, 0, lastErr
}

// enumerateDecodeCandidates returns the EC volumes at or below the decode
// threshold. It returns nothing when the route is disabled, and an error when the
// volume size limit is unknown, because fullness cannot be judged without it.
func (h *VacuumHandler) enumerateDecodeCandidates(topo *master_pb.TopologyInfo, cfg *Config, volumeSizeLimitMb uint64) ([]ecvacuum.Candidate, error) {
	if cfg.DecodeBelowFullnessPercent <= 0 {
		return nil, nil
	}
	candidates, err := ecvacuum.EnumerateDecodeCandidates(topo, cfg.CollectionFilter, cfg.DecodeBelowFullnessPercent, volumeSizeLimitMb)
	if err != nil {
		return nil, err
	}
	if cfg.DiskType != "" {
		candidates = filterCandidatesByDiskType(candidates, cfg.DiskType)
	}
	return candidates, nil
}

// Detect enumerates EC volumes that need action and proposes one job each: a
// decode for volumes that have shrunk below the fullness threshold, otherwise a
// vacuum for volumes whose deleted-needle ratio has crossed its threshold. It
// skips detection entirely when the last successful run is more recent than the
// minimum interval.
func (h *VacuumHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if jt := request.GetJobType(); jt != "" && jt != jobType {
		return fmt.Errorf("job type %q is not handled by %s worker", jt, jobType)
	}

	cfg := deriveConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())

	// Min-interval gate: skip when the last successful run is too recent.
	if last := request.GetLastSuccessfulRun(); last != nil && cfg.MinIntervalMinutes > 0 {
		if since := time.Since(last.AsTime()); since >= 0 && since < time.Duration(cfg.MinIntervalMinutes)*time.Minute {
			_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped",
				fmt.Sprintf("last successful run %s ago is within the %d-minute minimum interval; skipping", since.Round(time.Minute), cfg.MinIntervalMinutes), nil))
			return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true, TotalProposals: 0})
		}
	}

	masters := masterAddresses(request.GetClusterContext())
	topo, volumeSizeLimitMb, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return fmt.Errorf("fetch topology: %w", err)
	}

	candidates, err := enumerateGarbageEcVolumes(topo, cfg.CollectionFilter, cfg.GarbageThreshold)
	if err != nil {
		return err
	}
	if cfg.DiskType != "" {
		candidates = filterCandidatesByDiskType(candidates, cfg.DiskType)
	}

	// A volume below the decode threshold can also have cleared the vacuum
	// threshold — an EC volume holding little live data and a lot of garbage is
	// both — and the two actions cannot run against one shard set, so decoding
	// wins and the volume leaves the vacuum list. (A volume with *no* live needles
	// is not a vacuum candidate at all: the shared enumerator drops it, since only
	// a purge can reclaim it.)
	decodeCandidates, err := h.enumerateDecodeCandidates(topo, cfg, volumeSizeLimitMb)
	if err != nil {
		// Judging decode must not cost the vacuum detection that predates it.
		glog.Warningf("ec_vacuum: skipping decode detection: %v", err)
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("decode_skipped", err.Error(), nil))
		decodeCandidates = nil
	}
	// A volume whose shards span disk types is one candidate per group, and a
	// decode acts on the whole volume — DoEcDecode collects every shard, not just
	// the group's — so keep one proposal per volume: the second could only race
	// the first and fail.
	decodeCandidates = dedupeByVolume(decodeCandidates)

	if len(decodeCandidates) > 0 {
		decoded := make(map[uint32]bool, len(decodeCandidates))
		for _, c := range decodeCandidates {
			decoded[c.VolumeID] = true
		}
		kept := make([]ecvacuum.Candidate, 0, len(candidates))
		for _, c := range candidates {
			// Keyed on volume id alone, ignoring disk type: a volume whose shards
			// span disk types is two candidates, and decoding one group while
			// compacting the other would race on the same shard set.
			if !decoded[c.VolumeID] {
				kept = append(kept, c)
			}
		}
		candidates = kept
	}
	stats.ECVacuumJobsDetectedCounter.Add(float64(len(candidates)))
	stats.ECVacuumDecodeCandidatesDetectedCounter.Add(float64(len(decodeCandidates)))

	// Decode proposals lead, so a result cap spends its budget on the volumes that
	// free their whole footprint before the marginal compactions.
	proposals := buildProposals(decodeCandidates, modeDecode, volumeSizeLimitMb)
	proposals = append(proposals, buildProposals(candidates, modeVacuum, volumeSizeLimitMb)...)

	maxResults := int(request.GetMaxResults())
	if maxResults < 0 {
		maxResults = 0
	}
	hasMore := false
	if maxResults > 0 && len(proposals) > maxResults {
		proposals = proposals[:maxResults]
		hasMore = true
	}

	summary := fmt.Sprintf("EC volume maintenance detection: %d decode + %d vacuum candidate volume(s) (decode at or below %.0f%% full, compact over %.0f%% deleted)",
		len(decodeCandidates), len(candidates), cfg.DecodeBelowFullnessPercent*100, cfg.GarbageThreshold*100)
	if hasMore {
		summary += " (more available)"
	}
	if err := sender.SendActivity(pluginworker.BuildDetectorActivity("decision_summary", summary, nil)); err != nil {
		glog.V(1).Infof("ec_vacuum: failed to emit detection trace: %v", err)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   jobType,
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}
