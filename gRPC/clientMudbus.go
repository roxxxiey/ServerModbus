package gRPC

import (
	"context"
	"fmt"
	pb "github.com/roxxxiey/proto/go"
	"github.com/simonvetter/modbus"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type clientModbus struct {
	pb.UnimplementedPollDriverServiceServer
}

const (
	Type = "Modbus"
)

var registerType = map[string]modbus.RegType{
	"Holding_Registers": 0,
	"Input_Registers":   1,
}

func RegisterServerModbus(server *grpc.Server) {
	pb.RegisterPollDriverServiceServer(server, &clientModbus{})
}

func (s *clientModbus) PollType(ctx context.Context, request *pb.PollTypeRequest) (*pb.PollTypeResponse, error) {
	log.Println("calling pollType")
	return &pb.PollTypeResponse{
		Type: Type,
	}, nil
}

func (s *clientModbus) Poll(ctx context.Context, request *pb.PollRequest) (*pb.PollResponse, error) {
	log.Println("call Poll")

	modbusSettings := request.GetSettings()
	if modbusSettings == nil {
		log.Println("error: modbusSettings is nil")
		return nil, fmt.Errorf("modbusSettings is nil")
	}

	modbusMode := modbusSettings[0].GetValue()
	modbusAddress, err := strconv.ParseUint(modbusSettings[1].GetValue(), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("modbusAddress is invalid")
	}

	switch modbusMode {
	case "rtu":

		log.Println("RTU")
		if len(modbusSettings) != 7 {
			log.Println("error: len(modbusSettings) != 7")
			return nil, fmt.Errorf("len(modbusSettings) != 7")
		}

		comport := modbusSettings[2].GetValue()

		baudrate, err := strconv.ParseUint(modbusSettings[3].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse baudrate: %v", err)
		}

		parity, err := strconv.ParseUint(modbusSettings[4].GetValue(), 10, 32)
		if err != nil || checkParity(parity) == false {
			return nil, fmt.Errorf("failed to parse parity: %v", err)
		}

		spotbit, err := strconv.ParseUint(modbusSettings[5].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stopbit: %v", err)
		}

		databit, err := strconv.ParseUint(modbusSettings[6].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse databit: %v", err)
		}

		log.Printf("settings: %s, %d, %d, %d, %d", comport, baudrate, parity, spotbit, databit)
		url := fmt.Sprintf("%s://%s", modbusMode, comport)
		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:      url,
			Speed:    uint(baudrate),
			Parity:   uint(parity),
			StopBits: uint(spotbit),
			DataBits: uint(databit),
		})
		if err != nil {
			fmt.Errorf("call NewClient error:", err)
			return nil, err
		}
		err = client.SetUnitId(uint8(modbusAddress))
		if err != nil {
			fmt.Errorf("call SetUnitId error:", err)
		}
		err = client.Open()
		if err != nil {
			fmt.Errorf("call client.Open error:", err)
			return nil, err
		}
		defer client.Close()

		response, err := s.appeal(request, client)
		if err != nil {
			return nil, fmt.Errorf("appeal error: %v", err)
		}
		return response, nil

	case "rtuoverudp", "rtuovertcp":

		log.Println("Call OVERTCP or OVERUDP")
		if len(modbusSettings) != 4 {
			return nil, fmt.Errorf("len(modbusSettings) != 4")
		}

		ip := modbusSettings[2].GetValue()
		if isValidIPv4(ip) == false {
			return nil, fmt.Errorf("invalid ip")
		}
		port := modbusSettings[3].GetValue()

		url := fmt.Sprintf("%s://%s:%s", modbusMode, ip, port)

		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:     url,
			Timeout: 1 * time.Second,
		})
		log.Println(client)
		if err != nil {
			fmt.Errorf("call NewClient error:", err)
		}

		err = client.SetUnitId(uint8(modbusAddress))
		if err != nil {
			return nil, err
		}
		err = client.Open()
		if err != nil {
			fmt.Errorf("call client.Open error:", err)
		}
		defer client.Close()

		response, err := s.appeal(request, client)

		if err != nil {
			return nil, fmt.Errorf("response err:%v", err)
		}
		return response, nil

	default:

		log.Println("Don't know this protocol")
	}

	return nil, nil
}

func (s *clientModbus) ChangeMetric(ctx context.Context, request *pb.ChangeMetricRequest) (*pb.ChangeMetricResponse, error) {
	log.Println("calling change")

	modbusSettings := request.GetSettings()
	if modbusSettings == nil {
		log.Println("error: modbusSettings is nil")
		return nil, fmt.Errorf("modbusSettings is nil")
	}

	modbusMode := modbusSettings[0].GetValue()
	modbusAddress, err := strconv.ParseUint(modbusSettings[1].GetValue(), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("modbusAddress is invalid")
	}

	switch modbusMode {
	case "rtu":
		log.Println("RTU")
		if len(modbusSettings) != 7 {
			return nil, fmt.Errorf("invalid settings for tru (count != 7)")
		}

		comport := modbusSettings[2].GetValue()

		baudrate, err := strconv.ParseUint(modbusSettings[3].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse baudrate: %v", err)
		}

		parity, err := strconv.ParseUint(modbusSettings[4].GetValue(), 10, 32)
		if err != nil || checkParity(parity) == false {
			return nil, fmt.Errorf("failed to parse parity: %v", err)
		}

		spotbit, err := strconv.ParseUint(modbusSettings[5].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stopbit: %v", err)
		}

		databit, err := strconv.ParseUint(modbusSettings[6].GetValue(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse databit: %v", err)
		}

		log.Printf("settings: %s, %d, %d, %d, %d", comport, baudrate, parity, spotbit, databit)
		url := fmt.Sprintf("%s://%s", modbusMode, comport)
		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:      url,
			Speed:    uint(baudrate),
			Parity:   uint(parity),
			StopBits: uint(spotbit),
			DataBits: uint(databit),
		})
		if err != nil {
			fmt.Errorf("call NewClient error:", err)
			return nil, err
		}
		err = client.SetUnitId(uint8(modbusAddress))
		if err != nil {
			fmt.Errorf("call SetUnitId error:", err)
		}
		err = client.Open()
		if err != nil {
			fmt.Errorf("call client.Open error:", err)
			return nil, err
		}
		defer client.Close()

		response, err := s.changeData(request, client)
		if err != nil {
			return nil, fmt.Errorf("chanheData error: %v", err)
		}
		return response, nil

	case "rtuovertcp", "rtuooverudp":
		log.Println("Call OVERTCP or OVERUDP")
		if len(modbusSettings) != 4 {
			return nil, fmt.Errorf("len(modbusSettings) != 4")
		}

		ip := modbusSettings[2].GetValue()
		if isValidIPv4(ip) == false {
			return nil, fmt.Errorf("invalid ip")
		}
		port := modbusSettings[3].GetValue()

		url := fmt.Sprintf("%s://%s:%s", modbusMode, ip, port)

		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:     url,
			Timeout: 1 * time.Second,
		})
		log.Println(client)
		if err != nil {
			fmt.Errorf("call NewClient error:", err)
		}

		err = client.SetUnitId(uint8(modbusAddress))
		if err != nil {
			return nil, err
		}
		err = client.Open()
		if err != nil {
			fmt.Errorf("call client.Open error:", err)
		}
		defer client.Close()

		response, err := s.changeData(request, client)

		if err != nil {

			return nil, fmt.Errorf("chanheData err:%v", err)
		}
		return response, nil

	default:
		return nil, fmt.Errorf("modbusMode is invalid")
	}

	return nil, nil
}

func (s *clientModbus) Preset(ctx context.Context, request *pb.PresetRequest) (*pb.PresetResponse, error) {
	log.Println("calling preset")
	return nil, nil
}

func (s *clientModbus) appeal(request *pb.PollRequest, client *modbus.ModbusClient) (*pb.PollResponse, error) {
	log.Println("call appeal")
	pollItems := request.GetPollItems()
	var data []string
	for _, item := range pollItems {
		data = append(data, item.Addr)
	}

	var info []string
	for _, item := range data {
		inf := strings.Split(item, ":")
		log.Println(inf)
		regType := inf[3]
		mode := inf[0]

		regAddress, err := strconv.ParseUint(inf[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse register address in %s: %v", item, err)
		}

		quantity, err := strconv.ParseInt(inf[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse quantity in %s: %v", item, err)
		}

		forAdd, err := s.readRegister(mode, item, regType, uint16(quantity), uint16(regAddress), client)
		if err != nil {
			return nil, fmt.Errorf("failed to read register from %s: %v", item, err)
		}
		info = append(info, forAdd)

	}

	for i, item := range pollItems {
		item.Value = &info[i]
	}
	//log.Println(pollItems)
	return &pb.PollResponse{
		PollItem: pollItems,
	}, nil
}

func (s *clientModbus) readRegister(mode string, item string, regType string, quantity uint16, regAddress uint16, client *modbus.ModbusClient) (string, error) {
	log.Println("Call readRegister")
	switch regType {
	case "Output_Coils":
		switch mode {
		case "single":
			if quantity == 1 {
				read, err := client.ReadCoil(regAddress)
				return strconv.FormatBool(read), err
			}
			if quantity > 1 {
				read, err := client.ReadCoils(regAddress, quantity)
				if err != nil {
					return "", fmt.Errorf("failed to read coils: %v", err)
				}
				return typeConversionBool(read), err
			}
		case "multiple":
			read, err := client.ReadCoils(regAddress, quantity)
			if err != nil {
				return "", fmt.Errorf("failed to read coils: %v", err)
			}
			return typeConversionBool(read), err
		default:
			return "", fmt.Errorf("unknown modbus mode frame: %v", regType)
		}
	case "Input_Contacts":
		switch mode {
		case "single":
			if quantity == 1 {
				read, err := client.ReadDiscreteInput(regAddress)
				return strconv.FormatBool(read), err
			}
			if quantity > 1 {
				read, err := client.ReadDiscreteInputs(regAddress, quantity)
				if err != nil {
					return "", fmt.Errorf("failed to read contacts: %v", err)
				}
				return typeConversionBool(read), err
			}
		case "multiple":
			read, err := client.ReadDiscreteInputs(regAddress, quantity)
			if err != nil {
				return "", fmt.Errorf("failed to read contacts: %v", err)
			}
			return typeConversionBool(read), err
		default:
			return "", fmt.Errorf("unknown modbus mode frame: %v", regType)
		}
	case "Holding_Registers", "Input_Registers":
		switch mode {
		case "single":
			if quantity == 1 {
				p, err := client.ReadRegister(regAddress, registerType[regType])
				if err != nil {

					return "", fmt.Errorf("failed to read register %s: %v", item, err)
				}
				return strconv.Itoa(int(p)), err
			}
			if quantity > 1 {
				read, err := client.ReadRegisters(regAddress, quantity, registerType[regType])
				if err != nil {
					return "", fmt.Errorf("failed to read registers %s: %v", item, err)
				}
				log.Println(read)
				return typeConversionUint(read), err
			}
		case "multiple":
			read, err := client.ReadRegisters(regAddress, quantity, registerType[regType])
			if err != nil {
				return "", fmt.Errorf("failed to read registers %s: %v", item, err)
			}
			log.Println(read)
			return typeConversionUint(read), err
		default:
			return "", fmt.Errorf("unknown modbus mode frame: %v", regType)
		}
	default:
		return "", fmt.Errorf("invalid register type: %v", registerType)
	}
	return "", nil
}

func (s *clientModbus) changeData(request *pb.ChangeMetricRequest, client *modbus.ModbusClient) (*pb.ChangeMetricResponse, error) {
	log.Println("call changeData")

	pollItems := request.GetPollItem()
	var data []string
	var value []*string
	for _, item := range pollItems {
		data = append(data, item.Addr)
		value = append(value, item.Value)
	}
	for i, item := range data {
		inf := strings.Split(item, ":")
		log.Println(inf)
		regType := inf[3]
		mode := inf[0]

		regAddress, err := strconv.ParseUint(inf[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse register address in %s: %v", item, err)
		}

		quantity, err := strconv.ParseInt(inf[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse quantity in %s: %v", item, err)
		}

		err = s.writeRegisters(mode, item, regType, uint16(quantity), uint16(regAddress), client, value[i])

	}

	return &pb.ChangeMetricResponse{
		Status: "All information write correctly",
	}, nil
}

func (s *clientModbus) writeRegisters(mode string, item string, regType string, quantity uint16, regAddress uint16, client *modbus.ModbusClient, value *string) error {

	log.Println("call writeRegisters")
	switch regType {
	case "Output_Coils":
		switch mode {
		case "single":
			if quantity == 1 {

				p, err := strconv.ParseBool(*value)
				if err != nil {
					return fmt.Errorf("failed to parse register %s: %v", item, err)
				}

				err = client.WriteCoil(regAddress, p)
				if err != nil {
					return fmt.Errorf("failed to write coil: %s: %v", item, err)
				}
			}
			if quantity > 1 {

				p := stringToBoolArray(value)

				err := client.WriteCoils(regAddress, p)
				if err != nil {
					return fmt.Errorf("failed to write coilS: %s: %v", item, err)
				}

			}
		case "multiple":
			p := stringToBoolArray(value)

			err := client.WriteCoils(regAddress, p)
			if err != nil {
				return fmt.Errorf("failed to write coilS: %s: %v", item, err)
			}
		default:
			return fmt.Errorf("unknown modbus mode frame: %v", mode)
		}
	case "Holding_Registers":
		switch mode {
		case "single":
			if quantity == 1 {

				p, err := strconv.ParseUint(*value, 16, 32)
				log.Println(p)
				if err != nil {
					return fmt.Errorf("failed to parse register %s: %v", item, err)
				}

				err = client.WriteRegister(regAddress, uint16(p))
				if err != nil {
					return fmt.Errorf("failed to write register %s: %v", item, err)
				}

			}
			if quantity > 1 {

				p := stringToUint16Array(value)

				err := client.WriteRegisters(regAddress, p)
				if err != nil {
					return fmt.Errorf("failed to write registers: %s: %v", item, err)
				}

			}
		case "multiple":

			p := stringToUint16Array(value)

			err := client.WriteRegisters(regAddress, p)
			if err != nil {
				return fmt.Errorf("failed to write registers: %s: %v", item, err)
			}

		default:
			return fmt.Errorf("unknown modbus mode frame: %v", regType)
		}
	default:
		return fmt.Errorf("unknown register type: %v", regType)
	}
	return nil
}

func typeConversionUint(read []uint16) string {
	strArr := make([]string, len(read))
	for i, v := range read {
		log.Printf("Past strArr[%d]: %v\n]", i, strArr[i])
		strArr[i] = strconv.Itoa(int(v))
		log.Printf("NOw strArr[%d]: %v\n]", i, strArr[i])
	}
	return strings.Join(strArr, ",")
}

func typeConversionBool(read []bool) string {
	var res []string
	for _, v := range read {

		res = append(res, strconv.FormatBool(v))
	}
	return strings.Join(res, ",")
}

func checkParity(parity uint64) bool {
	if parity == 0 || parity == 1 || parity == 2 {
		return true
	} else {
		return false
	}
}

func isValidIPv4(ip string) bool {
	// Проверяем, является ли IP действительным и не является ли он nil
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}

	// Проверяем, что это IPv4, а не IPv6
	if strings.Contains(ip, ":") {
		return false
	}

	// Проверяем, что IP в формате x.x.x.x и каждая часть от 0 до 255
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 3 {
			return false
		}
	}

	return true
}

func stringToBoolArray(str *string) []bool {
	mass := strings.Split(*str, ",")
	log.Println(mass)
	boolMass := make([]bool, len(mass))
	for i, item := range mass {
		t, _ := strconv.ParseBool(item)
		boolMass[i] = t
	}
	log.Println(boolMass)
	return boolMass
}

func stringToUint16Array(str *string) []uint16 {
	mass := strings.Split(*str, ",")
	uintMass := make([]uint16, len(mass))
	for i, m := range mass {
		t, _ := strconv.ParseUint(m, 16, 32)
		uintMass[i] = uint16(t)
	}
	return uintMass
}
