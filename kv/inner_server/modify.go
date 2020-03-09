package inner_server

// ModifyType is the smallest unit of mutation of TinyKV's underlying storage (i.e., raw key/values on disk(s))
type ModifyType int64

const (
	ModifyTypePut    ModifyType = 1
	ModifyTypeDelete ModifyType = 2
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

func (m *Modify) Key() []byte {
	switch m.Type {
	case ModifyTypePut:
		return m.Data.(Put).Key
	case ModifyTypeDelete:
		return m.Data.(Delete).Key
	}
	return nil
}

func (m *Modify) Cf() string {
	switch m.Type {
	case ModifyTypePut:
		return m.Data.(Put).Cf
	case ModifyTypeDelete:
		return m.Data.(Delete).Cf
	}
	return ""
}
