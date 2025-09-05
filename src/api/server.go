package api

import (
	context "context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrServiceDoesNotExist = errors.New("service does not exist")
	ErrNameTaken           = errors.New("service name taken")
	Success                = int32(200)
	Failure                = int32(500)
)

type EndpointService struct {
	Name      string
	Instances []string
	Algo      string
}

func newEndpointService(name string, instances []string, algo string) *EndpointService {
	return &EndpointService{
		Name:      name,
		Instances: instances,
		Algo:      algo,
	}
}

type SDM struct {
	DataStore map[string]*EndpointService
}

// TODO: implement load balancing algorithms
func balance(options []string, lb string) string {
	switch lb {
	case "random":
		// implement random algo
		return options[0]
	case "url_hash":
		// implement least_conn algo
		return options[0]
	default:
		// default to round_robin
		return options[0]
	}
}
func existInList(item string, list []string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
func toEndpointList(instances []string) []*ServiceEndpoint {
	var res []*ServiceEndpoint
	for _, instance := range instances {
		parts := strings.Split(instance, ":")
		if len(parts) == 2 {
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}
			res = append(res, &ServiceEndpoint{
				IP:   parts[0],
				Port: int32(port),
			})
		}
	}
	return res
}
func NewSDM(size int) *SDM {
	return &SDM{
		DataStore: make(map[string]*EndpointService, size),
	}
}
func (s *SDM) nametaken(serviceName string) bool {
	_, ok := s.DataStore[serviceName]
	return ok
}
func (s *SDM) validAlgo(algo string) bool {
	switch strings.ToLower(algo) {
	case "round_robin", "random", "url_hash":
		return true
	default:
		return false
	}
}
func (s *SDM) registerService(serviceName string, endpoints []string, algo string) error {
	if s.nametaken(serviceName) {
		return ErrNameTaken
	}
	lbValue, ok := LoadBalance_value[strings.ToUpper(algo)]
	if !ok || LoadBalance(lbValue) == LoadBalance_LOAD_BALANCE_NONE || !s.validAlgo(algo) {
		algo = LoadBalance_name[int32(LoadBalance_LOAD_BALANCE_ROUND_ROBIN)]
	}

	newService := newEndpointService(serviceName, endpoints, algo)
	s.DataStore[serviceName] = newService
	return nil
}
func (s *SDM) addInstance(serviceName string, endpoint string) error {
	if !s.nametaken(serviceName) {
		return ErrServiceDoesNotExist
	}
	service := s.DataStore[serviceName]
	if existInList(endpoint, service.Instances) {
		fmt.Printf("Endpoint %s already exists for service %s\n", endpoint, serviceName)
		return nil // instance already exists
	}
	service.Instances = append(service.Instances, endpoint)
	s.DataStore[serviceName] = service
	return nil
}
func (s *SDM) batchAddInstance(serviceName string, endpoints []string) error {
	for _, endpoint := range endpoints {
		err := s.addInstance(serviceName, endpoint)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *SDM) unregisterService(serviceName string, endpoints []string) error {
	if !s.nametaken(serviceName) {
		return ErrServiceDoesNotExist
	}
	service := s.DataStore[serviceName]
	var updatedInstances []string
	for _, instance := range service.Instances {
		instaceExist := false
		for _, endpoint := range endpoints {
			if endpoint == instance {
				instaceExist = true
				break
			}
		}
		if !instaceExist {
			updatedInstances = append(updatedInstances, instance)
		}
	}
	service.Instances = updatedInstances
	s.DataStore[serviceName] = service
	return nil
}
func (s *SDM) deleteService(serviceName string) error {
	if !s.nametaken(serviceName) {
		return ErrServiceDoesNotExist
	}
	delete(s.DataStore, serviceName)
	return nil
}
func (s *SDM) renameService(serviceName string, newName string) error {
	if !s.nametaken(serviceName) {
		return ErrServiceDoesNotExist
	}
	if s.nametaken(newName) {
		return ErrNameTaken
	}
	service := s.DataStore[serviceName]
	delete(s.DataStore, serviceName)
	s.DataStore[newName] = newEndpointService(newName, service.Instances, service.Algo)
	return nil
}

// in the soon future make sure this actully uses the suplied methods
func (s *SDM) resolveService(serviceName string, many bool) []string {
	if !s.nametaken(serviceName) {
		return []string{}
	}
	service := s.DataStore[serviceName]
	if many {
		return service.Instances
	}
	if len(service.Instances) > 0 {
		return []string{service.Instances[0]}
	}
	return []string{}
}

/* return all services and their endpoints and algos*/
func (s *SDM) metrics() []*EndpointService {
	var res []*EndpointService
	for _, service := range s.DataStore {
		res = append(res, service)
	}
	return res
}
func removeDuplicates(endpoints []string) []string {
	unique := make(map[string]struct{})
	for _, endpoint := range endpoints {
		unique[endpoint] = struct{}{}
	}
	var res []string
	for endpoint := range unique {
		res = append(res, endpoint)
	}
	return res
}
func parseEndpoints(endpoints []*ServiceEndpoint) []string {
	var res []string
	for _, endpoint := range endpoints {
		endpoint.IP = strings.TrimSpace(endpoint.IP)
		strPort := strconv.Itoa(int(endpoint.Port))
		if endpoint.IP != "" && strPort != "" {
			res = append(res, endpoint.IP+":"+strPort)
		}
	}
	return res
}

type Server struct {
	UnimplementedServiceDiscoveryManagerServer
	Manager *SDM
}

func (s *Server) Ping(ctx context.Context, in *emptypb.Empty) (*PingReply, error) {
	return &PingReply{
		Message:   "pong",
		Timestamp: timestamppb.Now(),
	}, nil
}

/*
 */
func (s *Server) RegisterService(ctx context.Context, in *RegisterServiceRequest) (*GenericResponse, error) {
	fmt.Println("RegisterService (in) :", in)
	endpoints := parseEndpoints(in.Endpoints)
	endpoints = removeDuplicates(endpoints)

	err := s.Manager.registerService(in.ServiceName, endpoints, in.Algo.String())
	if err != nil {
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	return &GenericResponse{SuccessCode: Success, Message: "Service registered successfully"}, nil
}

/*
 */
func (s *Server) AddInstance(ctx context.Context, in *AddInstanceRequest) (*GenericResponse, error) {
	fmt.Println("AddInstance (in) :", in)
	endpoint := parseEndpoints(in.Endpoints)
	if len(endpoint) == 0 {
		err := errors.New("no endpoints provided")
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	err := s.Manager.batchAddInstance(in.ServiceName, endpoint)
	if err != nil {
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	return &GenericResponse{SuccessCode: Success, Message: "Instance added successfully"}, nil
}

/*
 */
func (s *Server) UnregisterService(ctx context.Context, in *RegisterServiceRequest) (*GenericResponse, error) {
	fmt.Println("UnregisterService (in) :", in)
	endpoints := parseEndpoints(in.Endpoints)
	err := s.Manager.unregisterService(in.ServiceName, endpoints)
	if err != nil {
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	return &GenericResponse{SuccessCode: Success, Message: "Instance(s) unregistered successfully"}, nil
}

/*
 */
func (s *Server) DeleteService(ctx context.Context, in *Service) (*GenericResponse, error) {
	fmt.Println("DeleteService (in) :", in)
	err := s.Manager.deleteService(in.ServiceName)
	if err != nil {
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	return &GenericResponse{SuccessCode: Success, Message: "Service deleted successfully"}, nil
}

/*
 */
func (s *Server) RenameService(ctx context.Context, in *RenameServiceRequest) (*GenericResponse, error) {
	fmt.Println("RenameService (in) :", in)
	err := s.Manager.renameService(in.OldServiceName, in.NewServiceName)
	if err != nil {
		return &GenericResponse{SuccessCode: Failure, Message: err.Error()}, err
	}
	return &GenericResponse{SuccessCode: Success, Message: "Service renamed successfully"}, nil
}

/*s
 */
func (s *Server) ResolveService(ctx context.Context, in *ResolveRequest) (*ResolveServiceResponse, error) {
	fmt.Println("ResolveService (in) :", in)
	endPoints := s.Manager.resolveService(in.ServiceName, in.ResolveMany)

	return &ResolveServiceResponse{Endpoints: toEndpointList(endPoints)}, nil
}

/*
 */
func (s *Server) Metrics(ctx context.Context, in *emptypb.Empty) (*MetricsResponse, error) {
	fmt.Println("Metrics (in) :", in)
	rawServices := s.Manager.metrics()
	services := encodeEndPoints(rawServices)
	return &MetricsResponse{Services: services}, nil
}

func encodeEndPoints(endPointService []*EndpointService) []*MetricsResponseServiceMetric {
	var res []*MetricsResponseServiceMetric
	for _, service := range endPointService {
		v := MetricsResponseServiceMetric{
			ServiceName:  service.Name,
			NumEndpoints: int32(len(service.Instances)),
			Algo:         LoadBalance(LoadBalance_value[strings.ToUpper(service.Algo)]),
			Endpoints:    toEndpointList(service.Instances),
		}
		res = append(res, &v)
	}
	return res
}
