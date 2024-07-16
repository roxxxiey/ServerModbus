package gRPC

import (
	"context"
	"fmt"
	pb "github.com/roxxxiey/proto/go"
	"github.com/simonvetter/modbus"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"strings"
	"time"
)

type serverModbus struct {
	pb.UnimplementedPollDriverServer
}

func RegisterServerMudbus(server *grpc.Server) {
	pb.RegisterPollDriverServer(server, &serverModbus{})
}
func (s *serverModbus) Poll(ctx context.Context, request *pb.PollRequest) (*pb.PollResponse, error) {
	log.Println("call Poll")

	modbusSettings := request.GetSettings()

	modbusAddr := modbusSettings[0].GetValue()

	switch modbusAddr {
	case "rtu":

		log.Println("RTU")
		comport := modbusSettings[1].GetValue()
		baudrate, err := strconv.ParseUint(modbusSettings[2].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse baudrate")
		}
		parity, _ := strconv.ParseUint(modbusSettings[3].GetValue(), 10, 32)
		spotbit, _ := strconv.ParseUint(modbusSettings[4].GetValue(), 10, 32)
		databit, _ := strconv.ParseUint(modbusSettings[5].GetValue(), 10, 32)

		url := fmt.Sprintf("%s://%s", modbusAddr, comport)
		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:      url,
			Speed:    uint(baudrate),
			Parity:   uint(parity),
			StopBits: uint(spotbit),
			DataBits: uint(databit),
		})
		if err != nil {
			log.Println("call NewClient error:", err)
			return nil, err
		}
		err = client.Open()
		if err != nil {
			log.Println("call client.Open error:", err)
		}
		defer client.Close()

		response, err := s.appeal(request, client)
		if err != nil {
			log.Fatalf("response err:%v", err)
		}
		return response, nil

	case "rtuoverudp", "rtuovertcp":

		log.Println("Call OVERTCP")
		ip := modbusSettings[1].GetValue()
		port := modbusSettings[2].GetValue()

		url := fmt.Sprintf("%s://%s:%s", modbusAddr, ip, port)

		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:     url,
			Timeout: 1 * time.Second,
		})
		if err != nil {
			log.Println("call NewClient error:", err)
		}
		err = client.Open()
		if err != nil {
			log.Println("call client.Open error:", err)
		}
		defer client.Close()
		response, err := s.appeal(request, client)

		if err != nil {
			log.Fatalf("OID() err: %v", err)
		}
		return response, nil

	default:
		log.Println("Don't know this protocol")
	}

	return nil, nil
}
func (s *serverModbus) ChangeMetric(ctx context.Context, request *pb.ChangeMetricRequest) (*pb.ChangeMetricResponse, error) {
	log.Println("calling change")
	return nil, nil
}

func (s *serverModbus) Preset(ctx context.Context, request *pb.PresetRequest) (*pb.PresetResponse, error) {
	log.Println("calling preset")
	return nil, nil
}

func (s *serverModbus) appeal(request *pb.PollRequest, client *modbus.ModbusClient) (*pb.PollResponse, error) {
	log.Println("call appeal")
	var data []string
	for _, item := range request.GetPollItem() {
		data = append(data, item.Addr)
	}
	log.Println("data was created")
	log.Println(data)

	var registerType = map[string]modbus.RegType{
		"HOLDING_REGISTER": 0,
		"INPUT_REGISTER":   1,
	}

	var info []string
	for _, item := range data {
		inf := strings.Split(item, ":")
		log.Println(inf)
		regType := inf[3]
		switch inf[0] {
		case "single":
			regAddress, _ := strconv.ParseUint(inf[1], 10, 32)
			quantity, err := strconv.ParseInt(inf[2], 10, 32)

			log.Printf("Quantity: %d\n", quantity)
			log.Printf("Address: %d\n", regAddress)

			if err != nil {
				fmt.Println("Error parsing integer:", err)
				continue
			}

			if quantity == 1 {
				log.Printf("work quantity == 1")
				p, _ := client.ReadRegister(uint16(regAddress), registerType[regType])
				log.Printf("p: %v\n", p)
				forAdd := strconv.Itoa(int(p))
				log.Printf("forAdd: %v\n", forAdd)
				info = append(info, forAdd)
				log.Printf("info: %v\n", info)
			}
			if quantity > 1 {
				read, _ := client.ReadRegisters(uint16(regAddress), uint16(quantity), registerType[regType])
				log.Printf("read: %v\n", read)
				strArr := make([]string, len(read))
				for i, v := range read {
					log.Printf("Past strArr[%d]: %v\n]", i, strArr[i])
					strArr[i] = strconv.Itoa(int(v))
					log.Printf("NOw strArr[%d]: %v\n]", i, strArr[i])
				}
				result := strings.Join(strArr, ",")
				log.Printf("result: %v\n", result)

				info = append(info, result)
				log.Printf("info: %v\n", info)
			}
		case "multiple":
			quantity, err := strconv.ParseInt(inf[2], 10, 32)
			if err != nil {
				fmt.Println("Error parsing integer:", err)
				continue
			}
			regAddress, _ := strconv.ParseUint(inf[1], 10, 32)

			read, err := client.ReadRegisters(uint16(regAddress), uint16(quantity), registerType[regType])
			if err != nil {
				log.Println("Error reading registers:", err)
			}

			strArr := make([]string, len(read))
			for i, v := range read {
				strArr[i] = strconv.Itoa(int(v))
			}
			result := strings.Join(strArr, ",")

			info = append(info, result)

		default:
			fmt.Println("i don't this modbus mode, i know only `Single` and `Multiple`")
		}
	}

	response := &pb.PollResponse{PollValue: make([]*pb.PollValue, len(data))}

	for i, item := range request.GetPollItem() {
		response.PollValue[i] = &pb.PollValue{
			Addr:  item.Addr,
			Name:  item.Name,
			Value: info[i],
		}
	}

	return response, nil
}
