package connection

import "github.com/BigbearO/mem_cache/tool/conf"

type VirtualConnection struct {
	KeepConnection
	dbIndex int
}

func NewVirtualConn() *VirtualConnection {
	c := &VirtualConnection{}
	return c
}

func (v *VirtualConnection) SetDBIndex(index int) {
	v.dbIndex = index
}

func (v *VirtualConnection) GetDBIndex() int {
	return v.dbIndex
}

func (v *VirtualConnection) GetPassword() string {
	return conf.GlobalConfig.RequirePass
}
