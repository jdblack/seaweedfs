package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandClusterHealth{})
}

// cluster.health is a read-only roll-up of the issues that quietly accumulate
// on a running cluster: unbalanced volumes and EC shards, replicas that went
// missing, garbage that never got vacuumed, and empty volumes still holding
// slots. Everything here is derived from one topology fetch, so it is cheap
// enough to run on a schedule; the expensive filer-versus-volume cross-checks
// live in the volume.fsck / fs.verify commands the wrapper script chains.
type commandClusterHealth struct {
}

func (c *commandClusterHealth) Name() string {
	return "cluster.health"
}

func (c *commandClusterHealth) Help() string {
	return `summarize the cluster's health as a list of pass/warn/fail findings

	cluster.health [-imbalanceThreshold=0.15] [-garbageThreshold=0.3]
	               [-freeDiskSpaceThreshold=5] [-idleAge=24h]
	               [-detailed] [-maxDetails=20]

	Every finding is computed from a single topology snapshot; nothing is
	changed. Findings:

	  node inventory    masters, volume servers and filers that reported in
	  disk capacity     disks out of volume slots or short on free space
	  volume replication  regular volumes with too few/many/misplaced replicas
	  EC replication    EC volumes with missing or duplicated shards
	  EC sizing         EC volumes too small to be worth encoding, or whose
	                    shards disagree on size (a truncated or half copy)
	  garbage           volumes whose deleted bytes exceed the threshold
	  balance           volume and EC shard count spread across the servers
	  empty volumes     volumes with only a superblock, still holding a slot

	The verdict at the end is UNHEALTHY if any finding failed, DEGRADED if any
	warned, otherwise HEALTHY.

	Options:
	  -imbalanceThreshold:   max/min spread across servers before a disk type is
	                         reported as unbalanced (0.15 = 15%)
	  -garbageThreshold:     deleted-byte fraction at which a volume is reported
	                         as needing a vacuum
	  -freeDiskSpaceThreshold: percent of filesystem free space below which a
	                         disk is reported
	  -idleAge:              an empty volume untouched for this long is flagged
	                         as a volume.deleteEmpty candidate; a read-only
	                         volume untouched for this long is reported as cold
	  -ecMinVolumeSizeMB:    an EC volume whose data is smaller than this is
	                         reported as undersized (default 30, 0 disables)
	  -detailed:             print the offending volume/server names
	  -maxDetails:           cap the detailed lines printed per finding (0 = all)

	For the filer-versus-volume cross-checks (orphan needles in both
	directions), run cluster-health.sh --deep, which drives volume.fsck.
`
}

func (c *commandClusterHealth) HasTag(CommandTag) bool {
	return false
}

func (c *commandClusterHealth) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	healthCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	imbalanceThreshold := healthCommand.Float64("imbalanceThreshold", 0.15, "max/min spread before a disk type is unbalanced")
	garbageThreshold := healthCommand.Float64("garbageThreshold", 0.3, "deleted-byte fraction at which a volume needs a vacuum")
	freeDiskSpaceThreshold := healthCommand.Float64("freeDiskSpaceThreshold", 5, "percent of filesystem free space below which a disk is reported")
	idleAge := healthCommand.Duration("idleAge", 24*time.Hour, "age after which an empty or cold volume is reported")
	detailed := healthCommand.Bool("detailed", false, "print the offending volume and server names")
	maxDetails := healthCommand.Int("maxDetails", 20, "cap the detailed lines per finding; 0 for all")
	ecMinVolumeSizeMB := healthCommand.Int("ecMinVolumeSizeMB", 30, "EC volume data size below which the volume is reported as undersized; 0 disables")

	if err = healthCommand.Parse(args); err != nil {
		return err
	}

	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return fmt.Errorf("collect topology: %w", err)
	}

	nodes := collectHealthNodes(topologyInfo)
	masterCount := len(commandEnv.MasterClient.GetMasters(context.Background()))
	filerCount, filerErr := countRegisteredFilers(commandEnv)

	now := time.Now()
	report := &healthReport{
		writer:     writer,
		detailed:   *detailed,
		maxDetails: *maxDetails,
	}
	report.add(checkNodeInventory(nodes, masterCount, filerCount, filerErr))
	report.add(checkDiskCapacity(nodes, *freeDiskSpaceThreshold))
	report.add(checkVolumeReplication(nodes))
	report.add(checkEcReplication(nodes))
	report.add(checkEcSizing(nodes, int64(*ecMinVolumeSizeMB)<<20))
	report.add(checkGarbage(nodes, *garbageThreshold))
	report.add(checkBalance(nodes, *imbalanceThreshold))
	report.add(checkEmptyVolumes(nodes, now, *idleAge))

	fmt.Fprintf(writer, "cluster health, topology %q, volume size limit %d MB\n\n", topologyInfo.Id, volumeSizeLimitMb)
	report.print()

	return nil
}

func countRegisteredFilers(commandEnv *CommandEnv) (count int, err error) {
	err = commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})
		if err != nil {
			return err
		}
		count = len(resp.ClusterNodes)
		return nil
	})
	if err != nil {
		return -1, err
	}
	return count, nil
}

// healthSeverity orders the findings so the report can pick the worst one for
// the verdict, and so a finding reads the same wherever it is printed.
type healthSeverity int

const (
	healthOK healthSeverity = iota
	healthWarn
	healthFail
)

func (s healthSeverity) String() string {
	switch s {
	case healthFail:
		return "FAIL"
	case healthWarn:
		return "WARN"
	default:
		return "OK"
	}
}

type healthFinding struct {
	name    string
	status  healthSeverity
	summary string
	details []string
}

// render writes the finding, indenting details under it. maxDetails <= 0 means
// print them all.
func (f healthFinding) render(writer io.Writer, detailed bool, maxDetails int) {
	fmt.Fprintf(writer, "[%-4s] %s: %s\n", f.status, f.name, f.summary)
	if !detailed {
		return
	}
	for i, d := range f.details {
		if maxDetails > 0 && i >= maxDetails {
			fmt.Fprintf(writer, "         ... and %d more\n", len(f.details)-maxDetails)
			break
		}
		fmt.Fprintf(writer, "         - %s\n", d)
	}
}

type healthReport struct {
	writer     io.Writer
	detailed   bool
	maxDetails int
	findings   []healthFinding
}

func (r *healthReport) add(f healthFinding) {
	r.findings = append(r.findings, f)
}

func (r *healthReport) print() healthSeverity {
	worst := healthOK
	fails, warns := 0, 0
	for _, f := range r.findings {
		switch f.status {
		case healthFail:
			fails++
			worst = healthFail
		case healthWarn:
			warns++
			if worst < healthWarn {
				worst = healthWarn
			}
		}
		f.render(r.writer, r.detailed, r.maxDetails)
	}

	verdict := "HEALTHY"
	switch worst {
	case healthFail:
		verdict = "UNHEALTHY"
	case healthWarn:
		verdict = "DEGRADED"
	}
	fmt.Fprintf(r.writer, "\nRESULT: %s (%d failure(s), %d warning(s))\n", verdict, fails, warns)
	return worst
}

// healthNode is one volume server, with its disks folded by type exactly as the
// master reports them, so the checks below need no other input.
type healthNode struct {
	id      string
	address string
	dc      string
	rack    string
	disks   map[string]*master_pb.DiskInfo
}

func collectHealthNodes(topologyInfo *master_pb.TopologyInfo) []*healthNode {
	var nodes []*healthNode
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		nodes = append(nodes, &healthNode{
			id:      dn.Id,
			address: dn.Address,
			dc:      string(dc),
			rack:    string(rack),
			disks:   dn.DiskInfos,
		})
	})
	slices.SortFunc(nodes, func(a, b *healthNode) int {
		return strings.Compare(a.id, b.id)
	})
	return nodes
}

// diskTypeName shows an unnamed disk type as the hdd it is.
func diskTypeName(diskType string) string {
	if diskType == "" {
		return types.HddType
	}
	return diskType
}

func displayCollection(collection string) string {
	if collection == "" {
		return "default"
	}
	return collection
}

func displayDataCenter(dc string) string {
	if dc == "" {
		return "default"
	}
	return dc
}

// imbalance is the widest min..max spread of a count across the servers that
// carry a disk type. A cluster with too few items to spread meaningfully is not
// judged.
type imbalance struct {
	diskType string
	total    int64
	nodes    int
	min      int64
	max      int64
	minNode  string
	maxNode  string
}

func (im imbalance) fraction() float64 {
	if im.max <= 0 {
		return 0
	}
	return float64(im.max-im.min) / float64(im.max)
}

// judgeable requires at least two servers and enough items that an even spread
// would put one on each; otherwise the spread says nothing.
func (im imbalance) judgeable() bool {
	return im.nodes >= 2 && im.total >= int64(im.nodes)
}

// computeImbalance returns the most skewed disk type among byType, keyed
// diskType -> node -> count. Disk types are walked in sorted order so a tie
// between two types resolves the same way on every run.
func computeImbalance(byType map[string]map[string]int64) (worst imbalance, found bool) {
	diskTypes := make([]string, 0, len(byType))
	for diskType := range byType {
		diskTypes = append(diskTypes, diskType)
	}
	sort.Strings(diskTypes)

	for _, diskType := range diskTypes {
		perNode := byType[diskType]
		if len(perNode) < 2 {
			continue
		}
		im := imbalance{
			diskType: diskType,
			nodes:    len(perNode),
			min:      math.MaxInt64,
			max:      math.MinInt64,
		}
		for node, count := range perNode {
			im.total += count
			if count < im.min {
				im.min, im.minNode = count, node
			}
			if count > im.max {
				im.max, im.maxNode = count, node
			}
		}
		if !found || im.fraction() > worst.fraction() {
			worst, found = im, true
		}
	}
	return
}

// formatImbalance renders one disk type's spread for the detailed log.
func formatImbalance(what string, im imbalance) string {
	return fmt.Sprintf("%s %s: %d..%d across %d node(s) (%.1f%%), least %s, most %s",
		what, diskTypeName(im.diskType), im.min, im.max, im.nodes, im.fraction()*100, im.minNode, im.maxNode)
}

// humanBytes is a thin wrapper so the call sites read like the report they
// build.
func humanBytes(b uint64) string {
	return util.BytesToHumanReadable(b)
}

// criticalDiskSpacePercent is where a filesystem is close enough to full that
// writes start failing, as opposed to merely being tight.
const criticalDiskSpacePercent = 2.0

// lowDiskSpaceFloorBytes keeps a tiny test filesystem from tripping the free
// space check; below this the percentage is noise.
const lowDiskSpaceFloorBytes = 1 << 30

func checkNodeInventory(nodes []*healthNode, masterCount, filerCount int, filerErr error) healthFinding {
	f := healthFinding{name: "node inventory"}

	dcCounts := map[string]int{}
	for _, n := range nodes {
		dcCounts[displayDataCenter(n.dc)]++
	}
	dcNames := make([]string, 0, len(dcCounts))
	for dc := range dcCounts {
		dcNames = append(dcNames, dc)
	}
	sort.Strings(dcNames)
	dcParts := make([]string, 0, len(dcNames))
	for _, dc := range dcNames {
		dcParts = append(dcParts, fmt.Sprintf("%s=%d", dc, dcCounts[dc]))
	}

	f.summary = fmt.Sprintf("%d master(s), %d volume server(s)", masterCount, len(nodes))
	if len(dcParts) > 0 {
		f.summary += " (" + strings.Join(dcParts, ", ") + ")"
	}
	if filerCount < 0 {
		f.summary += ", filers unknown"
	} else {
		f.summary += fmt.Sprintf(", %d filer(s)", filerCount)
	}

	switch {
	case len(nodes) == 0:
		f.status = healthFail
		f.summary += " — no volume servers registered with the master"
	case masterCount == 0:
		f.status = healthFail
		f.summary += " — no masters reachable"
	case filerErr != nil:
		f.status = healthWarn
		f.summary += fmt.Sprintf(" — listing filers failed: %v", filerErr)
	case filerCount == 0:
		f.status = healthWarn
		f.summary += " — no filers registered, so filer-backed checks cannot run"
	default:
		f.status = healthOK
	}
	return f
}

func checkDiskCapacity(nodes []*healthNode, freeSpacePercent float64) healthFinding {
	f := healthFinding{name: "disk capacity"}
	var noSlots, lowSpace []string
	worstFreePct := math.MaxFloat64
	for _, n := range nodes {
		for diskType, disk := range n.disks {
			if disk.MaxVolumeCount > 0 && disk.FreeVolumeCount <= 0 {
				noSlots = append(noSlots, fmt.Sprintf("%s %s: 0 of %d volume slot(s) free", n.id, diskTypeName(diskType), disk.MaxVolumeCount))
			}
			if disk.DiskTotalBytes >= lowDiskSpaceFloorBytes {
				freePct := float64(disk.DiskFreeBytes) * 100 / float64(disk.DiskTotalBytes)
				if freePct < worstFreePct {
					worstFreePct = freePct
				}
				if freePct < freeSpacePercent {
					lowSpace = append(lowSpace, fmt.Sprintf("%s %s: %.1f%% free (%s of %s)",
						n.id, diskTypeName(diskType), freePct, humanBytes(disk.DiskFreeBytes), humanBytes(disk.DiskTotalBytes)))
				}
			}
		}
	}
	sort.Strings(noSlots)
	sort.Strings(lowSpace)

	parts := []string{fmt.Sprintf("%d disk(s) out of volume slots", len(noSlots))}
	if worstFreePct != math.MaxFloat64 {
		parts = append(parts, fmt.Sprintf("lowest free space %.1f%%", worstFreePct))
	} else {
		parts = append(parts, "free space not reported")
	}

	switch {
	case worstFreePct < criticalDiskSpacePercent:
		f.status = healthFail
		f.summary = "critically low disk space; " + strings.Join(parts, ", ")
	case len(noSlots) > 0 || len(lowSpace) > 0:
		f.status = healthWarn
		f.summary = strings.Join(parts, ", ")
	default:
		f.status = healthOK
		f.summary = "every disk has free volume slots and healthy free space"
	}
	f.details = append(f.details, noSlots...)
	f.details = append(f.details, lowSpace...)
	return f
}

// replicaGroup is one regular volume gathered across the servers that hold it.
type replicaGroup struct {
	collection string
	rp         *super_block.ReplicaPlacement
	nodes      map[string]struct{}
	locations  []balancer.Location
}

func checkVolumeReplication(nodes []*healthNode) healthFinding {
	f := healthFinding{name: "volume replication"}
	groups := map[uint32]*replicaGroup{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, v := range disk.VolumeInfos {
				g, ok := groups[v.Id]
				if !ok {
					rp, rpErr := super_block.NewReplicaPlacementFromByte(byte(v.ReplicaPlacement))
					if rpErr != nil {
						rp = nil
					}
					g = &replicaGroup{collection: v.Collection, rp: rp, nodes: map[string]struct{}{}}
					groups[v.Id] = g
				}
				if _, seen := g.nodes[n.id]; seen {
					continue
				}
				g.nodes[n.id] = struct{}{}
				g.locations = append(g.locations, balancer.Location{
					DataCenter: n.dc,
					Rack:       n.rack,
					NodeID:     n.id,
					Host:       n.address,
				})
			}
		}
	}

	var under, over, misplaced []string
	for vid, g := range groups {
		if g.rp == nil {
			continue
		}
		desired := g.rp.GetCopyCount()
		got := len(g.nodes)
		switch {
		case got < desired:
			under = append(under, fmt.Sprintf("volume %d (%s, %s): %d of %d replicas", vid, displayCollection(g.collection), g.rp.String(), got, desired))
		case got > desired:
			over = append(over, fmt.Sprintf("volume %d (%s, %s): %d of %d replicas", vid, displayCollection(g.collection), g.rp.String(), got, desired))
		}
		if got >= desired && !balancer.SatisfyReplicaCurrentLocation(g.rp, g.locations) {
			misplaced = append(misplaced, fmt.Sprintf("volume %d (%s, %s): replicas do not satisfy the placement", vid, displayCollection(g.collection), g.rp.String()))
		}
	}
	sort.Strings(under)
	sort.Strings(over)
	sort.Strings(misplaced)

	total := len(groups)
	switch {
	case len(under) > 0:
		f.status = healthFail
	case len(over) > 0 || len(misplaced) > 0:
		f.status = healthWarn
	default:
		f.status = healthOK
	}
	if len(under) == 0 && len(over) == 0 && len(misplaced) == 0 {
		f.summary = fmt.Sprintf("all %d regular volume(s) have the right replicas", total)
	} else {
		f.summary = fmt.Sprintf("%d under-replicated, %d over-replicated, %d misplaced out of %d regular volume(s)",
			len(under), len(over), len(misplaced), total)
	}
	f.details = append(f.details, under...)
	f.details = append(f.details, over...)
	f.details = append(f.details, misplaced...)
	if len(over) > 0 {
		f.details = append(f.details, "note: over-replication is expected while ec.encode or volume.balance runs, or when the master runs with replicationAsMin")
	}
	return f
}

// ecReplicaGroup is one EC volume gathered across the servers that hold shards
// of it, checked against the volume's own data+parity ratio.
type ecReplicaGroup struct {
	collection   string
	dataShards   int
	parityShards int
	shardNodes   map[erasure_coding.ShardId]map[string]struct{}
}

func (g *ecReplicaGroup) totalShards() int {
	return g.dataShards + g.parityShards
}

// presentShardCount returns how many expected shard ids are present on at least
// one node, and the ids that are missing everywhere. A shard present on two
// nodes still counts once: redundancy is not recoverability.
func (g *ecReplicaGroup) presentShardCount() (present int, missing []int) {
	for sid := 0; sid < g.totalShards(); sid++ {
		if len(g.shardNodes[erasure_coding.ShardId(sid)]) == 0 {
			missing = append(missing, sid)
		} else {
			present++
		}
	}
	return
}

// shardsAfterLosingNode returns, for one node, how many expected shards would
// still be present if it vanished: a shard counts only if another node holds it.
func (g *ecReplicaGroup) shardsAfterLosingNode(node string) (remaining int, held int) {
	for sid := 0; sid < g.totalShards(); sid++ {
		nodes := g.shardNodes[erasure_coding.ShardId(sid)]
		if _, ok := nodes[node]; !ok {
			continue
		}
		held++
		for other := range nodes {
			if other != node {
				remaining++
				break
			}
		}
	}
	return
}

// formatShardIds renders a shard id list compactly.
func formatShardIds(ids []int) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}

// ecVolumeGroups collects the shard placement of every EC volume, keyed by
// volume id, shared by the EC checks below.
func ecVolumeGroups(nodes []*healthNode) map[uint32]*ecReplicaGroup {
	groups := map[uint32]*ecReplicaGroup{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, eci := range disk.EcShardInfos {
				g, ok := groups[eci.Id]
				if !ok {
					g = &ecReplicaGroup{
						collection:   eci.Collection,
						dataShards:   erasure_coding.EcShardsVolumeDataShards(eci),
						parityShards: erasure_coding.EcShardsVolumeParityShards(eci),
						shardNodes:   map[erasure_coding.ShardId]map[string]struct{}{},
					}
					groups[eci.Id] = g
				}
				for _, sid := range erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci).Ids() {
					if g.shardNodes[sid] == nil {
						g.shardNodes[sid] = map[string]struct{}{}
					}
					g.shardNodes[sid][n.id] = struct{}{}
				}
			}
		}
	}
	return groups
}

func checkEcReplication(nodes []*healthNode) healthFinding {
	f := healthFinding{name: "EC replication"}
	groups := ecVolumeGroups(nodes)

	if len(groups) == 0 {
		f.status = healthOK
		f.summary = "no EC volumes"
		return f
	}

	// EC tolerates losing up to `parity` shards, so a missing parity shard is a
	// redundancy loss, not data loss. Only dropping below the data-shard count
	// makes a volume unreconstructable.
	var atRisk, degraded, over []string
	for vid, g := range groups {
		total := g.totalShards()
		present, missing := g.presentShardCount()

		var duplicated, unexpected []int
		for sid := 0; sid < total; sid++ {
			if len(g.shardNodes[erasure_coding.ShardId(sid)]) > 1 {
				duplicated = append(duplicated, sid)
			}
		}
		for sid := range g.shardNodes {
			if int(sid) >= total {
				unexpected = append(unexpected, int(sid))
			}
		}
		slices.Sort(duplicated)
		slices.Sort(unexpected)

		if len(missing) > 0 {
			base := fmt.Sprintf("EC volume %d (%s, %d+%d): %d of %d shards present, missing %s",
				vid, displayCollection(g.collection), g.dataShards, g.parityShards, present, total, formatShardIds(missing))
			if present < g.dataShards {
				atRisk = append(atRisk, base+"; fewer than the data shards, cannot reconstruct")
			} else {
				degraded = append(degraded, base+"; still reconstructable from the remaining shards")
			}
		}
		if len(duplicated) > 0 || len(unexpected) > 0 {
			over = append(over, fmt.Sprintf("EC volume %d (%s, %d+%d): duplicated shard(s) [%s], unexpected [%s]",
				vid, displayCollection(g.collection), g.dataShards, g.parityShards,
				formatShardIds(duplicated), formatShardIds(unexpected)))
		}
	}
	sort.Strings(atRisk)
	sort.Strings(degraded)
	sort.Strings(over)

	switch {
	case len(atRisk) > 0:
		f.status = healthFail
	case len(degraded) > 0 || len(over) > 0:
		f.status = healthWarn
	default:
		f.status = healthOK
	}
	if len(atRisk) == 0 && len(degraded) == 0 && len(over) == 0 {
		f.summary = fmt.Sprintf("all %d EC volume(s) have every shard exactly once", len(groups))
	} else {
		f.summary = fmt.Sprintf("%d unreconstructable, %d degraded, %d over-replicated out of %d EC volume(s)",
			len(atRisk), len(degraded), len(over), len(groups))
	}
	f.details = append(f.details, atRisk...)
	f.details = append(f.details, degraded...)
	f.details = append(f.details, over...)
	return f
}

// checkEcSizing reports two things the shard sizes the master already holds make
// free to see. A volume's EC shards are one even split of its data, so every
// data and parity shard carries the same number of bytes: the volume's original
// data size is the sum of its data-shard sizes, and any two shards that
// disagree mean one was truncated or only half copied. The first is a volume
// that was too small to be worth encoding; the second is damage the shard
// count alone cannot show.
func checkEcSizing(nodes []*healthNode, minVolumeSizeBytes int64) healthFinding {
	f := healthFinding{name: "EC sizing"}

	type ecSizing struct {
		collection   string
		dataShards   int
		parityShards int
		sizes        map[erasure_coding.ShardId]int64
	}
	groups := map[uint32]*ecSizing{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, eci := range disk.EcShardInfos {
				g, ok := groups[eci.Id]
				if !ok {
					g = &ecSizing{
						collection:   eci.Collection,
						dataShards:   erasure_coding.EcShardsVolumeDataShards(eci),
						parityShards: erasure_coding.EcShardsVolumeParityShards(eci),
						sizes:        map[erasure_coding.ShardId]int64{},
					}
					groups[eci.Id] = g
				}
				for _, s := range erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci).AsSlice() {
					size := int64(s.Size)
					// A volume server that has not heartbeated sizes yet reports
					// zero; skip it rather than read it as a tiny shard. When two
					// holders disagree, keep the larger, so one stale report does
					// not invent a mismatch.
					if size <= 0 {
						continue
					}
					if current, ok := g.sizes[s.Id]; !ok || size > current {
						g.sizes[s.Id] = size
					}
				}
			}
		}
	}

	if len(groups) == 0 {
		f.status = healthOK
		f.summary = "no EC volumes"
		return f
	}

	var undersized, mismatched []string
	for vid, g := range groups {
		var dataSize int64
		distinct := map[int64]struct{}{}
		for sid, size := range g.sizes {
			distinct[size] = struct{}{}
			if int(sid) < g.dataShards {
				dataSize += size
			}
		}
		if minVolumeSizeBytes > 0 && dataSize > 0 && dataSize < minVolumeSizeBytes {
			undersized = append(undersized, fmt.Sprintf("EC volume %d (%s, %d+%d): %s of data, below the %s minimum",
				vid, displayCollection(g.collection), g.dataShards, g.parityShards,
				humanBytes(uint64(dataSize)), humanBytes(uint64(minVolumeSizeBytes))))
		}
		if len(distinct) > 1 {
			sizes := make([]int64, 0, len(distinct))
			for size := range distinct {
				sizes = append(sizes, size)
			}
			sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
			parts := make([]string, len(sizes))
			for i, size := range sizes {
				parts[i] = humanBytes(uint64(size))
			}
			mismatched = append(mismatched, fmt.Sprintf("EC volume %d (%s): shards disagree on size (%s)",
				vid, displayCollection(g.collection), strings.Join(parts, ", ")))
		}
	}
	sort.Strings(undersized)
	sort.Strings(mismatched)

	f.details = append(f.details, undersized...)
	f.details = append(f.details, mismatched...)

	switch {
	case len(undersized) == 0 && len(mismatched) == 0:
		f.status = healthOK
		f.summary = fmt.Sprintf("all %d EC volume(s) hold a full volume's worth, every shard matching", len(groups))
	case len(undersized) == 0:
		f.status = healthWarn
		f.summary = fmt.Sprintf("%d of %d EC volume(s) have shards that disagree on size",
			len(mismatched), len(groups))
	case len(mismatched) == 0:
		f.status = healthWarn
		f.summary = fmt.Sprintf("%d of %d EC volume(s) undersized (<%s)",
			len(undersized), len(groups), humanBytes(uint64(minVolumeSizeBytes)))
	default:
		f.status = healthWarn
		f.summary = fmt.Sprintf("%d of %d EC volume(s) undersized (<%s); %d with shards that disagree on size",
			len(undersized), len(groups), humanBytes(uint64(minVolumeSizeBytes)), len(mismatched))
	}
	return f
}

// checkEcNodeLoss reports EC volumes that would become unreconstructable if any
// single server were lost, even though every shard is present right now. The
// topology exposes this for free, and ec.balance fixes it.
func checkEcNodeLoss(nodes []*healthNode) healthFinding {
	f := healthFinding{name: "EC node-loss tolerance"}
	groups := ecVolumeGroups(nodes)
	if len(groups) == 0 {
		f.status = healthOK
		f.summary = "no EC volumes"
		return f
	}

	var fragile []string
	for vid, g := range groups {
		present, _ := g.presentShardCount()
		if present < g.dataShards {
			// already unreconstructable; the EC replication check reports it
			continue
		}

		nodeIds := map[string]struct{}{}
		for _, addrs := range g.shardNodes {
			for node := range addrs {
				nodeIds[node] = struct{}{}
			}
		}
		sortedNodes := make([]string, 0, len(nodeIds))
		for node := range nodeIds {
			sortedNodes = append(sortedNodes, node)
		}
		sort.Strings(sortedNodes)

		for _, node := range sortedNodes {
			remaining, held := g.shardsAfterLosingNode(node)
			if remaining < g.dataShards {
				fragile = append(fragile, fmt.Sprintf(
					"EC volume %d (%s, %d+%d): losing %s (holds %d shard(s)) would leave %d, below the %d needed to reconstruct",
					vid, displayCollection(g.collection), g.dataShards, g.parityShards, node, held, remaining, g.dataShards))
				break // one fragile node per volume is enough to flag it
			}
		}
	}
	sort.Strings(fragile)

	if len(fragile) == 0 {
		f.status = healthOK
		f.summary = fmt.Sprintf("every EC volume survives the loss of any one server (%d volume(s))", len(groups))
		return f
	}
	f.status = healthWarn
	f.summary = fmt.Sprintf("%d of %d EC volume(s) could not survive a single server loss; ec.balance spreads the shards",
		len(fragile), len(groups))
	f.details = fragile
	return f
}

// checkReplicaConsistency looks for replicas of one volume that disagree about
// their own metadata, or that land twice on the same server - neither of which
// the placement checks above can see, because those only count replicas.
func checkReplicaConsistency(nodes []*healthNode) healthFinding {
	f := healthFinding{name: "replica consistency"}

	type replicaSet struct {
		collection  string
		rpBytes     map[uint32]struct{}
		collections map[string]struct{}
		perNode     map[string]int
	}
	sets := map[uint32]*replicaSet{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, v := range disk.VolumeInfos {
				s, ok := sets[v.Id]
				if !ok {
					s = &replicaSet{
						collection:  v.Collection,
						rpBytes:     map[uint32]struct{}{},
						collections: map[string]struct{}{},
						perNode:     map[string]int{},
					}
					sets[v.Id] = s
				}
				s.rpBytes[v.ReplicaPlacement] = struct{}{}
				s.collections[v.Collection] = struct{}{}
				s.perNode[n.id]++
			}
		}
	}

	var inconsistent []string
	for vid, s := range sets {
		var problems []string
		if len(s.rpBytes) > 1 {
			specs := make([]string, 0, len(s.rpBytes))
			for rb := range s.rpBytes {
				if rp, err := super_block.NewReplicaPlacementFromByte(byte(rb)); err == nil {
					specs = append(specs, rp.String())
				} else {
					specs = append(specs, fmt.Sprintf("byte %d", rb))
				}
			}
			sort.Strings(specs)
			problems = append(problems, "replica placements disagree ("+strings.Join(specs, ", ")+")")
		}
		if len(s.collections) > 1 {
			names := make([]string, 0, len(s.collections))
			for c := range s.collections {
				names = append(names, displayCollection(c))
			}
			sort.Strings(names)
			problems = append(problems, "collections disagree ("+strings.Join(names, ", ")+")")
		}
		for node, count := range s.perNode {
			if count > 1 {
				problems = append(problems, fmt.Sprintf("%d replicas on %s", count, node))
			}
		}
		if len(problems) > 0 {
			sort.Strings(problems)
			inconsistent = append(inconsistent, fmt.Sprintf("volume %d (%s): %s",
				vid, displayCollection(s.collection), strings.Join(problems, "; ")))
		}
	}
	sort.Strings(inconsistent)

	if len(inconsistent) == 0 {
		f.status = healthOK
		f.summary = fmt.Sprintf("every volume's replicas agree (%d volume(s))", len(sets))
		return f
	}
	f.status = healthWarn
	f.summary = fmt.Sprintf("%d of %d volume(s) have replicas that disagree", len(inconsistent), len(sets))
	f.details = inconsistent
	return f
}

type garbageVolume struct {
	vid        uint32
	collection string
	ratio      float64
	deleted    uint64
}

func checkGarbage(nodes []*healthNode, threshold float64) healthFinding {
	f := healthFinding{name: "garbage"}
	// Replicas agree, but dedupe by volume id anyway so a volume is counted
	// once and reported from its worst copy.
	worst := map[uint32]*master_pb.VolumeInformationMessage{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, v := range disk.VolumeInfos {
				if prev, ok := worst[v.Id]; !ok || v.DeletedByteCount > prev.DeletedByteCount {
					worst[v.Id] = v
				}
			}
		}
	}

	var offenders []garbageVolume
	var reclaimable uint64
	for vid, v := range worst {
		if v.Size == 0 {
			continue
		}
		ratio := float64(v.DeletedByteCount) / float64(v.Size)
		if ratio < threshold {
			continue
		}
		reclaimable += v.DeletedByteCount
		offenders = append(offenders, garbageVolume{vid: vid, collection: v.Collection, ratio: ratio, deleted: v.DeletedByteCount})
	}
	slices.SortFunc(offenders, func(a, b garbageVolume) int {
		return int(b.deleted) - int(a.deleted)
	})

	if len(offenders) == 0 {
		f.status = healthOK
		f.summary = fmt.Sprintf("no regular volume is above %.0f%% garbage (%d volume(s) scanned)", threshold*100, len(worst))
		return f
	}
	f.status = healthWarn
	f.summary = fmt.Sprintf("%d of %d regular volume(s) above %.0f%% garbage, %s reclaimable by volume.vacuum",
		len(offenders), len(worst), threshold*100, humanBytes(reclaimable))
	for _, o := range offenders {
		f.details = append(f.details, fmt.Sprintf("volume %d (%s): %.1f%% garbage, %s reclaimable",
			o.vid, displayCollection(o.collection), o.ratio*100, humanBytes(o.deleted)))
	}
	return f
}

func checkBalance(nodes []*healthNode, threshold float64) healthFinding {
	f := healthFinding{name: "balance"}
	volByType := map[string]map[string]int64{}
	ecByType := map[string]map[string]int64{}
	for _, n := range nodes {
		for diskType, disk := range n.disks {
			if volByType[diskType] == nil {
				volByType[diskType] = map[string]int64{}
			}
			if ecByType[diskType] == nil {
				ecByType[diskType] = map[string]int64{}
			}
			volByType[diskType][n.id] += disk.VolumeCount
			var shards int64
			for _, eci := range disk.EcShardInfos {
				shards += int64(erasure_coding.GetShardCount(eci))
			}
			ecByType[diskType][n.id] += shards
		}
	}

	volIm, volFound := computeImbalance(volByType)
	ecIm, ecFound := computeImbalance(ecByType)
	volBad := volFound && volIm.judgeable() && volIm.fraction() > threshold
	ecBad := ecFound && ecIm.judgeable() && ecIm.fraction() > threshold

	describe := func(what string, im imbalance, found, bad bool) string {
		if !found {
			return what + " not reported"
		}
		if !im.judgeable() {
			return fmt.Sprintf("%s too sparse to judge (%d across %d node(s))", what, im.total, im.nodes)
		}
		state := "evenly spread"
		if bad {
			state = "unbalanced"
		}
		return fmt.Sprintf("%s %s: %.1f%% spread across %d node(s)", what, state, im.fraction()*100, im.nodes)
	}
	volText := describe("regular volumes", volIm, volFound, volBad)
	ecText := describe("EC shards", ecIm, ecFound, ecBad)

	if volBad || ecBad {
		f.status = healthWarn
		f.summary = fmt.Sprintf("%s; %s (threshold %.0f%%)", volText, ecText, threshold*100)
	} else {
		f.status = healthOK
		f.summary = fmt.Sprintf("%s; %s", volText, ecText)
	}
	if volFound {
		f.details = append(f.details, formatImbalance("regular volumes", volIm))
	}
	if ecFound {
		f.details = append(f.details, formatImbalance("EC shards", ecIm))
	}
	return f
}

type emptyVolume struct {
	vid        uint32
	collection string
	node       string
	modified   int64
}

func checkEmptyVolumes(nodes []*healthNode, now time.Time, idleAge time.Duration) healthFinding {
	f := healthFinding{name: "empty volumes"}
	nowUnix := now.Unix()
	idleSeconds := int64(idleAge / time.Second)

	// A volume whose .dat holds only its superblock carries no data; it is what
	// volume.deleteEmpty reclaims. Dedupe by id so each volume counts once.
	seen := map[uint32]emptyVolume{}
	for _, n := range nodes {
		for _, disk := range n.disks {
			for _, v := range disk.VolumeInfos {
				if v.Size > super_block.SuperBlockSize {
					continue
				}
				if _, ok := seen[v.Id]; ok {
					continue
				}
				seen[v.Id] = emptyVolume{vid: v.Id, collection: v.Collection, node: n.id, modified: v.ModifiedAtSecond}
			}
		}
	}

	var idle, fresh []string
	for _, e := range seen {
		label := fmt.Sprintf("volume %d (%s) on %s", e.vid, displayCollection(e.collection), e.node)
		if e.modified > 0 && e.modified+idleSeconds < nowUnix {
			idle = append(idle, label+fmt.Sprintf(", idle %s", (time.Duration(nowUnix-e.modified)*time.Second).String()))
		} else {
			fresh = append(fresh, label)
		}
	}
	sort.Strings(idle)
	sort.Strings(fresh)

	switch {
	case len(idle) > 0:
		f.status = healthWarn
		f.summary = fmt.Sprintf("%d empty volume(s) idle over %s (volume.deleteEmpty candidates), %d recently created",
			len(idle), idleAge, len(fresh))
	case len(fresh) > 0:
		f.status = healthOK
		f.summary = fmt.Sprintf("%d empty volume(s), all created within %s", len(fresh), idleAge)
	default:
		f.status = healthOK
		f.summary = "no empty volumes"
	}
	f.details = append(f.details, idle...)
	f.details = append(f.details, fresh...)
	return f
}
