package inner_server

type ModifyType int64

const (
	ModifyTypePut    ModifyType = 0
	ModifyTypeDelete ModifyType = 1
)

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}

type Modify struct {
	Type ModifyType
	Data interface{}
}
