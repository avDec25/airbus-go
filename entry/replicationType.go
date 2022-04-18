package entry

type ReplicationTypeEnum uint

const (
	Local = iota
	Global
	MasterToSlave
	SlaveToMaster
	Stop
)

var replicationTypeStrings = []string{
	"LOCAL",
	"GLOBAL",
	"MASTERTOSLAVE",
	"SLAVETOMASTER",
	"STOP",
}

func (gt ReplicationTypeEnum) Name() string {
	return replicationTypeStrings[gt]
}

func (gt ReplicationTypeEnum) Ordinal() int {
	return int(gt)
}

func (gt ReplicationTypeEnum) Values() *[]string {
	return &replicationTypeStrings
}

func (gt ReplicationTypeEnum) valueOf(input string) ReplicationTypeEnum {
	if input == "" {
		return gt
	}
	for index, value := range replicationTypeStrings {
		if input == value {
			switch index {
			case Local:
				return Local
			case Global:
				return Global
			case MasterToSlave:
				return MasterToSlave
			case SlaveToMaster:
				return SlaveToMaster
			case Stop:
				return Stop
			}
		}
	}
	return gt
}
