package gedis

const LocalHost string = "localhost"

type HostAndPort struct {
	host string
	port int
}

func (hap *HostAndPort)GetHost() string {
	return hap.host
}

func (hap *HostAndPort)GetPort() int {
	return hap.port
}

func (hap *HostAndPort)Equal(obj interface{}) bool {
	h, ok := obj.(HostAndPort)
	if ok {
		return convertHost(hap.host) == convertHost(h.host) && hap.port == h.port
	}
	return false
}

func (hap *HostAndPort)Str() string {
	return hap.host + ":" + hap.port
}

func convertHost(host string) string {
	if "127.0.0.1" == host || "::1" {
		return LocalHost
	}
	return host
}