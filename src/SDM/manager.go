package ServiceDiscoveryManager

import (
	"RouteFinder/api"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

type ip string

type SDMConfigBuilder struct {
	service     ip
	timeout     time.Duration
	retryCount  int
	logLocation string
}
type SDMConfig struct {
	service ip
	timeout time.Duration
	// exponetional back off for x retries
	retryCount  int
	logLocation string
}

func NewSDMConfig() SDMConfigBuilder {
	return SDMConfigBuilder{}
}
func (b SDMConfigBuilder) Build() SDMConfig {
	if b.service == "" {
		b.service = ip(os.Getenv("SDM_SERVICE"))
	}
	if b.timeout == time.Duration(0) {
		b.timeout = 3 * time.Second
	}
	if b.retryCount <= 0 || b.retryCount > 8 {
		b.retryCount = 2
	}
	if b.logLocation == "" {
		b.logLocation = "sdm.log"
	}
	return SDMConfig(b)
}
func (b SDMConfigBuilder) WithService(service ip) SDMConfigBuilder {
	b.service = service
	return b
}
func (b SDMConfigBuilder) WithTimeout(timeout time.Duration) SDMConfigBuilder {
	b.timeout = timeout
	return b
}
func (b SDMConfigBuilder) WithRetryCount(retryCount int) SDMConfigBuilder {
	b.retryCount = retryCount
	return b
}
func (b SDMConfigBuilder) WithLogLocation(logLocation string) SDMConfigBuilder {
	b.logLocation = logLocation
	return b
}

type SDM struct {
	service ip
	cache   map[string][]string // local service name to list of IPs
}

func NewManager(s SDMConfig) *SDM {
	return &SDM{service: s.service, cache: make(map[string][]string)}
}

// mabey add a pool of connections so a new conn isnt made each function call

// manager just acts as a cache for the distributed Service Discovery system
func (Manager *SDM) Register(serviceName string, instances []string, algo string) error {
	_, ok := Manager.cache[serviceName]
	if ok {
		fmt.Printf("Service %s already registered\n", serviceName)
		return nil // no op
	}
	conn, err := grpc.NewClient(string(Manager.service), grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := api.NewServiceDiscoveryManagerClient(conn)
	genericRespo, _ := client.RegisterService(context.Background(), &api.RegisterServiceRequest{
		ServiceName: serviceName,
		Endpoints:   api.InstanceToEndpoints(instances),
	})
	if genericRespo.SuccessCode != api.Success {
		return errors.New(genericRespo.Message)
	}
	Manager.cache[serviceName] = instances
	return nil
}
func (Manager *SDM) Rename(oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	conn, err := grpc.NewClient(string(Manager.service), grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := api.NewServiceDiscoveryManagerClient(conn)
	genericResponse, _ := client.RenameService(context.Background(), &api.RenameServiceRequest{
		OldServiceName: oldName,
		NewServiceName: newName,
	})
	if genericResponse.SuccessCode != api.Success {
		return errors.New(genericResponse.Message)
	}
	instances, exists := Manager.cache[oldName]
	if exists {
		delete(Manager.cache, oldName)
		Manager.cache[newName] = instances
	}
	return nil

}

func (Manager *SDM) Unregister(serviceName string, instances []string) error {
	conn, err := grpc.NewClient(string(Manager.service), grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := api.NewServiceDiscoveryManagerClient(conn)
	genericRespo, _ := client.UnregisterService(context.Background(), &api.RegisterServiceRequest{
		ServiceName: serviceName,
		Endpoints:   api.InstanceToEndpoints(instances),
	})
	if genericRespo.SuccessCode != api.Success {
		return errors.New(genericRespo.Message)
	}
	cachedInstances, exists := Manager.cache[serviceName]
	if exists {
		// remove instances from cache
		instanceMap := make(map[string]bool)
		for _, inst := range instances {
			instanceMap[inst] = true
		}
		var updatedInstances []string
		for _, inst := range cachedInstances {
			if !instanceMap[inst] {
				updatedInstances = append(updatedInstances, inst)
			}
		}
		if len(updatedInstances) == 0 {
			delete(Manager.cache, serviceName)
		} else {
			Manager.cache[serviceName] = updatedInstances
		}
	}
	return nil
}

/*Doesnt have to be registered by this instance check with distributed system*/
func (Manager *SDM) discover(serviceName string) ([]string, error) {
	instances, exists := Manager.cache[serviceName]
	if exists && len(instances) > 0 {
		return instances, nil
	}
	conn, err := grpc.NewClient(string(Manager.service), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := api.NewServiceDiscoveryManagerClient(conn)
	resolveResp, err := client.ResolveService(context.Background(), &api.ResolveRequest{
		ServiceName: serviceName,
	})
	if err != nil {
		return nil, err
	}
	var res []string
	for _, service := range resolveResp.Endpoints {
		endpoint := service.IP + ":" + strconv.Itoa(int(service.Port))
		res = append(res, endpoint)

	}
	Manager.cache[serviceName] = res
	if len(res) == 0 {
		return nil, errors.New("no instances found for service " + serviceName)
	}
	return res, nil
}

// lots of parsing happening here
func (Manager *SDM) DoRequest(req http.Request) (*http.Response, error) {
	curHost := req.URL.Host
	hosts, err := Manager.discover(curHost)
	if err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		return nil, errors.New("no application servers registered under " + curHost)
	}
	// pick first host for now
	req.URL.Host = hosts[0]
	client := &http.Client{
		Timeout: 2 * time.Second,
	}
	return client.Do(&req)
}
