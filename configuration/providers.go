package configuration

type DataDirProvider interface {
	DataDir() string
}
