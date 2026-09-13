package ec_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/stats"
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

	// fetchTopology and transport are seams so unit tests can drive detection and
	// compaction without a live cluster.
	fetchTopology func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error)
	transport     shardTransport
}

// NewEcVacuumHandler creates the EC vacuum handler. workingDir is the worker's
// -workingDir, where a volume's shards are collected and compacted.
func NewEcVacuumHandler(dialOpt grpc.DialOption, workingDir string) *VacuumHandler {
	return &VacuumHandler{
		grpcDialOption: dialOpt,
		workingDir:     workingDir,
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			topo, _, err := fetchTopologyFromMasters(ctx, masters, dialOpt)
			return topo, err
		},
		transport: newClusterTransport(dialOpt),
	}
}

func (h *VacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "EC Vacuum",
		Description:             "Reclaim space by removing deleted needles from EC volumes, compacting on the worker",
		Weight:                  70,
	}
}

func (h *VacuumHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "EC Vacuum",
		Description:       "Detect EC volumes whose deleted-needle ratio is high, then compact their shards on a worker.",
		Icon:              "fas fa-broom",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-vacuum-admin",
			Title:       "EC Vacuum Admin Config",
			Description: "Admin-side scope and threshold for EC vacuum detection.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before a volume is selected for vacuum.",
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
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				fieldCollectionFilter: {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldDiskType:         {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldGarbageThreshold: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: defaultGarbageThreshold}},
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
							Description: "Bound one volume's collect → compact → redistribute cycle (3600 = one hour).",
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

// Detect enumerates EC volumes whose deleted-needle ratio has crossed the
// threshold and proposes one vacuum per qualifying volume. It skips detection
// entirely when the last successful run is more recent than the minimum
// interval.
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
	topo, err := h.fetchTopology(ctx, masters)
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
	stats.ECVacuumJobsDetectedCounter.Add(float64(len(candidates)))

	maxResults := int(request.GetMaxResults())
	if maxResults < 0 {
		maxResults = 0
	}
	proposals, hasMore := buildProposals(candidates, maxResults)

	summary := fmt.Sprintf("EC vacuum detection: %d candidate volume(s) over %.0f%% deleted", len(proposals), cfg.GarbageThreshold*100)
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
