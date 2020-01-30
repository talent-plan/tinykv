package inner_server

// ModifyType is the smallest unit of mutation of TinyKV's underlying storage (i.e., raw key/values on disk(s))
type ModifyType int64

const (
	ModifyTypePut    ModifyType = 1
	ModifyTypeDelete ModifyType = 2
)

const (
	CfDefault string = "default"
	CfLock    string = "lock"
	CfWrite   string = "write"
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
