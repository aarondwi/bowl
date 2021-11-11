package bowl

type State int32

const (
	ACTIVE         State = 0
	MARKED_REMOVED State = 1
)

func (s State) canBeRemoved() bool {
	return s&MARKED_REMOVED == MARKED_REMOVED
}
