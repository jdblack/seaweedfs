package ec_bitrot_scan

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  jobType,
		Category: pluginworker.CategoryDefault,
		Aliases:  []string{"ec-bitrot", "ec.bitrot", "ec_bitrot_scrub"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewBitrotScanHandler(opts.GrpcDialOption, opts.WorkingDir), nil
		},
	})
}

// BitrotScanHandler is the plugin job handler for EC bitrot scanning.
type BitrotScanHandler struct {
	grpcDialOption grpc.DialOption

	// workingDir holds the rotation bookmark. Empty disables the cursor: every
	// cycle then starts at a random point in the roster.
	workingDir string

	// fetchTopology and repair are seams so unit tests can drive detection and
	// repair without a live cluster.
	fetchTopology func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error)
	repair        repairer

	// randIntn picks the starting index for a cycle that has no cursor. A seam
	// so tests can pin the cold start; nil falls back to math/rand.
	randIntn func(n int) int
}

func NewBitrotScanHandler(dialOpt grpc.DialOption, workingDir string) *BitrotScanHandler {
	return &BitrotScanHandler{
		grpcDialOption: dialOpt,
		workingDir:     workingDir,
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			topo, _, err := fetchTopologyFromMasters(ctx, masters, dialOpt)
			return topo, err
		},
		repair: newClusterRepairer(dialOpt),
	}
}

// fetchTopologyFromMasters returns a topology snapshot (and the master's volume
// size limit in MB) from the first reachable master.
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

func (h *BitrotScanHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "EC Bitrot Scan",
		Description:             "Verify EC shard bytes against the per-block checksum sidecar, including cold parity shards",
		Weight:                  40,
	}
}

func (h *BitrotScanHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "EC Bitrot Scan",
		Description:       "Scheduled verification of every EC shard's bytes against the per-block CRC32C checksum sidecar, with opt-in parity-bounded repair.",
		Icon:              "fas fa-shield-virus",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-bitrot-scan-admin",
			Title:       "EC Bitrot Scan Admin Config",
			Description: "Admin-side scope and safety controls for EC bitrot scanning.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before a volume is selected for scanning.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldCollectionFilter,
							Label:       "Collection Filter",
							Description: "Only scan EC volumes in matching collections. Comma-separated names, wildcards, or regex patterns. Empty = all.",
							Placeholder: "all collections",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        fieldDiskType,
							Label:       "Disk Type",
							Description: "Only scan EC shards on this disk type (e.g. hdd, ssd). Empty = all.",
							Placeholder: "all disks",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
				{
					SectionId:   "coverage",
					Title:       "Coverage",
					Description: "Which EC volume parts the scheduled scan verifies.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldCheckIndex,
							Label:       "Check Index",
							Description: "Also verify .ecx index integrity alongside shard checksums. Off = shard checksums only. Index issues are reported, never auto-repaired.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
					},
				},
				{
					SectionId:   "repair",
					Title:       "Repair",
					Description: "Read-only by default. Auto repair quarantines Reed-Solomon-confirmed corrupt shards so ec.rebuild regenerates them.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldAutoRepair,
							Label:       "Auto Repair",
							Description: "When on, quarantine corrupt shards (unmount + delete, bounded by the volume's parity count) so ec.rebuild regenerates a clean copy. Off = read-only.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				fieldCollectionFilter: {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldDiskType:         {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				fieldAutoRepair:       {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: false}},
				fieldCheckIndex:       {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-bitrot-scan-worker",
			Title:       "EC Bitrot Scan Worker Config",
			Description: "Worker-side scanning cadence and timeout.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "cadence",
					Title:       "Cadence",
					Description: "Controls how frequently detection proposes new scans.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        fieldScanIntervalMins,
							Label:       "Full Sweep Interval (minutes)",
							Description: "Target time for one full pass over every EC volume (10080 = 7 days). Detection derives each cycle's batch size from this and the detection interval, so the pass stays on schedule as the fleet grows.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        fieldMinIntervalMins,
							Label:       "Minimum Interval (minutes, optional)",
							Description: "Optional floor on how often this job type may run: detection is skipped when the last successful run is more recent than this. Leave 0. A value longer than the detection interval is ignored, because it would stretch one sweep over several intervals.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        fieldScanTimeoutSecs,
							Label:       "Scan Timeout (s)",
							Description: "Maximum time to scrub one volume across all of its shard holders.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				fieldScanIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanIntervalMinutes}},
				fieldMinIntervalMins:  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinIntervalMinutes}},
				fieldScanTimeoutSecs:  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanTimeoutSeconds}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalMinutes:      10,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           500,
			GlobalExecutionConcurrency:    8,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           30,
			JobTypeMaxRuntimeSeconds:      1800,
			ExecutionTimeoutSeconds:       1800,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			fieldScanIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanIntervalMinutes}},
			fieldMinIntervalMins:  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinIntervalMinutes}},
			fieldScanTimeoutSecs:  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultScanTimeoutSeconds}},
		},
	}
}

// Detect walks the EC volume roster as a rotation: it resumes at the bookmark
// written by the previous cycle, proposes the next slice of volumes (one scan
// per distinct (volume_id, collection, disk_type), wrapping to the front of the
// roster), and advances the bookmark past what it proposed. The slice is
// derived from the configured sweep interval and the admin's detection
// interval, so one full pass keeps its length as the roster grows.
func (h *BitrotScanHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if jt := request.GetJobType(); jt != "" && jt != jobType {
		return fmt.Errorf("job type %q is not handled by ec_bitrot_scan worker", jt)
	}

	cfg := deriveConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())

	// The admin's detection interval is both the sweep clock and the yardstick
	// for whether a configured floor would stretch it.
	tickMinutes := int(request.GetAdminRuntime().GetDetectionIntervalMinutes())

	// Optional floor on how often this job type may run. The sweep interval,
	// not this, is what paces the rotation; effectiveFloorMinutes drops a floor
	// that would stretch the pass.
	if floor := effectiveFloorMinutes(cfg, tickMinutes); floor > 0 &&
		pluginworker.ShouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), floor*60) {
		since := time.Since(request.GetLastSuccessfulRun().AsTime())
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped",
			fmt.Sprintf("last successful run %s ago is within the %d-minute minimum interval; skipping", since.Round(time.Minute), floor), nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true, TotalProposals: 0})
	}

	masters := masterAddresses(request.GetClusterContext())
	topo, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return fmt.Errorf("fetch topology: %w", err)
	}

	candidates, err := enumerateEcVolumes(topo, cfg.CollectionFilter)
	if err != nil {
		return err
	}
	if cfg.DiskType != "" {
		candidates = filterCandidatesByDiskType(candidates, cfg.DiskType)
	}

	maxResults := int(request.GetMaxResults())
	if maxResults < 0 {
		maxResults = 0
	}
	slice, cycles := scanSliceSize(len(candidates), cfg.ScanIntervalMinutes, tickMinutes, maxResults)

	// A cycle with no cursor starts at a random point in the roster rather than
	// at its head. A fresh detector — a recreated pod whose working directory
	// was wiped, or an admin restart that moved the detector lease to another
	// worker — would otherwise walk the same first volumes every time, deferring
	// the rest of the roster by a full pass on each restart.
	bm := readBookmark(h.workingDir)
	start := bookmarkPosition(candidates, bm)
	if bm == nil {
		start = h.coldStartIndex(len(candidates))
	}
	proposals, hasMore := buildProposals(candidates, slice, start)
	h.advanceBookmark(candidates, start, slice)

	glog.V(1).Infof("ec_bitrot_scan: roster=%d slice=%d cycles_per_pass=%d start_index=%d tick=%dm sweep_interval=%dm",
		len(candidates), slice, cycles, start, tickMinutes, cfg.ScanIntervalMinutes)

	summary := fmt.Sprintf("EC bitrot scan detection: proposing %d of %d volume(s)", len(proposals), len(candidates))
	if hasMore {
		summary += fmt.Sprintf(" (slice %d per cycle; full pass %d cycle(s))", slice, cycles)
	}
	details := map[string]*plugin_pb.ConfigValue{
		"roster_size":     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(candidates))}},
		"slice":           {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(slice)}},
		"cycles_per_pass": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(cycles)}},
		"sweep_start":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: sweepStartKey(candidates, start)}},
	}
	if err := sender.SendActivity(pluginworker.BuildDetectorActivity("decision_summary", summary, details)); err != nil {
		glog.V(1).Infof("ec_bitrot_scan: failed to emit detection trace: %v", err)
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

// coldStartIndex returns where a cycle with no cursor begins. Random, so that
// repeated cold starts spread across the roster instead of converging on its
// head; the rotation still walks the deterministic roster order from there.
func (h *BitrotScanHandler) coldStartIndex(rosterSize int) int {
	if rosterSize <= 0 {
		return 0
	}
	if h.randIntn != nil {
		return h.randIntn(rosterSize)
	}
	return rand.Intn(rosterSize)
}

// advanceBookmark records the volume this cycle stopped at, so the next cycle
// resumes after it. Best-effort by design: a failed write costs one restart of
// the rotation, never a skipped volume.
func (h *BitrotScanHandler) advanceBookmark(candidates []ecVolumeCandidate, start, slice int) {
	if len(candidates) == 0 || slice <= 0 {
		return
	}
	taken := slice
	if taken > len(candidates) {
		taken = len(candidates)
	}
	if start < 0 || start >= len(candidates) {
		start = 0
	}
	last := candidates[(start+taken-1)%len(candidates)]
	writeBookmark(h.workingDir, &scanBookmark{
		VolumeID:   last.VolumeID,
		Collection: last.Collection,
		DiskType:   last.DiskType,
	})
}

// sweepStartKey names the volume a cycle starts at, for the detector trace. An
// operator can compare it against the previous cycle's to see the rotation
// moving, or against a volume that looks stale to see whether it has been
// reached yet.
func sweepStartKey(candidates []ecVolumeCandidate, start int) string {
	if len(candidates) == 0 || start < 0 || start >= len(candidates) {
		return ""
	}
	return candidates[start].dedupeKey()
}

// masterAddresses extracts the master gRPC addresses from a detection/execution
// cluster context.
func masterAddresses(cc *plugin_pb.ClusterContext) []string {
	if cc == nil {
		return nil
	}
	return cc.GetMasterGrpcAddresses()
}

// filterCandidatesByDiskType keeps only candidates on the requested disk type.
func filterCandidatesByDiskType(candidates []ecVolumeCandidate, diskType string) []ecVolumeCandidate {
	filtered := candidates[:0:0]
	for _, c := range candidates {
		if c.DiskType == diskType {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
