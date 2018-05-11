package main

import (
	"os"
	"net"
	"time"
	"strconv"
	"encoding/json"

	"github.com/op/go-logging"
	"github.com/chepeftw/bchainlibs"
	"fmt"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("router")

var me = net.ParseIP(bchainlibs.LocalhostAddr)
var sendMsgCount = 0
var sizeMsgCount = 0

// +++++++++ Routing Protocol
var forwarded = make(map[string]bool)
var packets = make(map[string]bchainlibs.Packet)

// +++++++++ Channels
var input = make(chan string)
var output = make(chan string)
var blockchain = make(chan string)
var miner = make(chan string)
var raft = make(chan string)
var resending = make(chan int)
var done = make(chan bool)

func sendMessage(payload bchainlibs.Packet) {
	bchainlibs.SendGeneric(output, payload, log)
	log.Debug("Sending Packet with ID " + payload.ID + " to channel output")

	go func() {
		time.Sleep(time.Second * time.Duration(1))
		log.Debug("RE attendResendingChannel => 1")
		resending <- 1
	}()
}

func sendInternal(name string, payload bchainlibs.Packet, channel chan string) {
	bchainlibs.SendGeneric(channel, payload, log)
	log.Debug("Sending Packet with ID " + payload.ID + " to channel " + name)
}

func sendBlockchain(payload bchainlibs.Packet) {
	sendInternal("blockchain", payload, blockchain)
}
func sendMiner(payload bchainlibs.Packet) {
	sendInternal("miner", payload, miner)
}
func sendRaft(payload bchainlibs.Packet) {
	sendInternal("raft", payload, raft)
}

// Function that handles the output channel
func attendOutputChannel() {
	log.Info("Starting output channel")

	Server, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.BroadcastAddr+bchainlibs.RouterPort)
	bchainlibs.CheckError(err, log)
	Local, err := net.ResolveUDPAddr(bchainlibs.Protocol, me.String()+bchainlibs.LocalPort)
	bchainlibs.CheckError(err, log)
	Conn, err := net.DialUDP(bchainlibs.Protocol, Local, Server)
	bchainlibs.CheckError(err, log)
	defer Conn.Close()

	for {
		j, more := <-output
		if more {
			if Conn != nil {
				buf := []byte(j)
				_, err = Conn.Write(buf)

				sizeMsgCount += len(buf)
				sendMsgCount += 1

				log.Debug(me.String() + " " + j + " MESSAGE_SIZE=" + strconv.Itoa(len(buf)))
				log.Debug(me.String() + " SENDING_MESSAGE=1")
				bchainlibs.CheckError(err, log)
			}
		} else {
			fmt.Println("closing channel")
			return
		}
	}
}

func attendInternalChannel(name string, port string, channel <-chan string) {
	log.Info("Starting " + name + " channel")
	bchainlibs.SendToNetwork(me.String(), port, channel, false, log, me)
}

func attendBlockchainChannel() {
	attendInternalChannel("blockchain", bchainlibs.BlockchainPort, blockchain)
}
func attendMinerChannel() {
	attendInternalChannel("miner", bchainlibs.MinerPort, miner)
}
func attendRaftChannel() {
	attendInternalChannel("raft", bchainlibs.RaftPort, raft)
}

func attendResendingChannel() {
	log.Debug("Starting resending channel")
	for {
		_, more := <-resending
		if more {
			missingAck := 0
			for v, k := range forwarded {
				if !k {
					missingAck = missingAck + 1
					log.Info("Packet " + v + " not acked yet!")
					log.Debug("RE_SEND_MESSAGE=1")
					sendMessage(packets[v])
				}
			}

			if missingAck > 0 {
				go func() {
					time.Sleep(time.Second * time.Duration(1))
					log.Debug("RE attendResendingChannel => 1")
					resending <- 1
				}()
			}
		}
	}
}

// Function that handles the buffer channel
func attendInputChannel() {
	log.Debug("Starting input channel")
	for {
		j, more := <-input
		if more {
			// First we take the json, unmarshal it to an object
			payload := bchainlibs.Packet{}
			json.Unmarshal([]byte(j), &payload)

			//log.Debug("---------------------------")
			//log.Debug("Something arrived")
			//log.Debug(j)

			source := payload.Source
			id := payload.ID

			log.Debug("---------------------------")
			log.Debug("Something arrived with id = " + id + " and type = " + strconv.Itoa(payload.Type))

			if "" == id {
				log.Error("Packet has no ID ... weird :(")
				continue
			}

			switch payload.Type {

			case bchainlibs.InternalPong:
				log.Info("Receving PONG = " + id)
				break

			case bchainlibs.InternalQueryType:
				if _, ok := forwarded[ "q"+id ]; !ok {
					log.Info("Receiving InternalQueryType Packet")
					// This is the start of the query, which later can be queried in MongoDB as the minimal value of this.
					// Then compared to the maximun I can get the time it took to propagate.
					// This is fine cause there is just one query
					log.Debug("QUERY_ID=" + payload.Query.ID)
					log.Debug("QUERY_TIME_SENT_" + payload.Query.ID + "=" + strconv.FormatInt(time.Now().UnixNano(), 10))
					payload.Type = bchainlibs.QueryType
					forwarded[ "q"+id ] = false
					packets["q"+id] = payload

					sendBlockchain(payload)
					sendMessage(payload)
				}
				break

				// ----------------------------
				// From X
				// ----------------------------

			case bchainlibs.QueryType:
				sizeMsgCount = 0
				sendMsgCount = 0

				if _, ok := forwarded[ "q"+id ]; !ok && !eqIp(me, source) {
					log.Info("Receiving QueryType Packet")
					log.Debug("QUERY_TIME_RECEIVED_" + payload.Query.ID + "=" + strconv.FormatInt(time.Now().UnixNano(), 10))
					forwarded[ "q"+id ] = true

					payload.Query.Hops = payload.Query.Hops + 1
					log.Debug("QUERY_HOPS_COUNT_" + payload.Query.ID + "=" + strconv.Itoa(payload.Query.Hops))

					sendBlockchain(payload)
					sendMessage(payload)
				} else if !forwarded[ "q"+id ] {
					forwarded[ "q"+id ] = true
				}
				break

			case bchainlibs.TransactionType:
				if _, ok := forwarded[ "t"+id ]; !ok && !eqIp(me, source) {
					log.Info("Receiving TransactionType Packet")
					forwarded[ "t"+id ] = true
					sendBlockchain(payload)
					sendMiner(payload)
					sendMessage(payload)
				} else if !forwarded[ "t"+id ] {
					forwarded[ "t"+id ] = true
				}
				break

			case bchainlibs.BlockType:
				if _, ok := forwarded[ "b"+id ]; !ok && !eqIp(me, source) {
					log.Info("Receiving BlockType Packet")
					forwarded[ "b"+id ] = true
					sendBlockchain(payload)
					sendMessage(payload)

					log.Debug("QUERY_MESSAGES_SIZE=" + strconv.Itoa(sizeMsgCount) + "|||| QUERY_MESSAGES_SEND=" + strconv.Itoa(sendMsgCount))
				} else if !forwarded[ "b"+id ] {
					forwarded[ "b"+id ] = true
				}
				break

				// ----------------------------
				// From Blockchain
				// ----------------------------

			case bchainlibs.LastBlockType:
				log.Info("Receiving LastBlockType Packet")
				sendMiner(payload)
				break

			case bchainlibs.LaunchElection:
				log.Info("Receiving LaunchElection Packet")

				start := bchainlibs.CreateRaftStartPacket(me)
				sendRaft(start)

				sendMiner(payload)
				break

			case bchainlibs.InternalTransactionType:
				log.Info("Receiving InternalTransactionType Packet")

				payload.Type = bchainlibs.TransactionType
				forwarded["t"+id] = false
				packets["t"+id] = payload

				sendBlockchain(payload)
				sendMiner(payload)
				sendMessage(payload)
				//log.Debug("TRANSACTION_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + id)
				break

				// ----------------------------
				// From Miner
				// ----------------------------

			case bchainlibs.InternalBlockType:
				log.Info("Receiving InternalBlockType Packet")

				payload.Type = bchainlibs.BlockType
				forwarded["b"+id] = false
				packets["b"+id] = payload

				sendBlockchain(payload)
				sendMessage(payload)
				//log.Debug("BLOCK_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + id)
				break

				// ----------------------------
				// From Raft
				// ----------------------------

			case bchainlibs.RaftResult:
				log.Info("Receiving ResultFromElection Packet")
				sendMiner(payload)

				stop := bchainlibs.CreateRaftStopPacket(me)
				sendRaft(stop)
				break

			}

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

func eqIp(a net.IP, b net.IP) bool {
	return bchainlibs.CompareIPs(a, b)
}

func pingInternals() {
	time.Sleep(time.Second * time.Duration(2))

	payload := bchainlibs.BuildPing(me)
	sendBlockchain(payload)

	time.Sleep(time.Second * time.Duration(1))

	payload = bchainlibs.BuildPing(me)
	sendMiner(payload)
}

func main() {

	confPath := "/app/conf.yml"
	if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
	}
	var c bchainlibs.Conf
	c.GetConf(confPath)

	targetSync := c.TargetSync
	logPath := c.LogPath

	// Logger configuration
	logName := "router"
	f := bchainlibs.PrepareLogGen(logPath, logName, "data")
	defer f.Close()
	f2 := bchainlibs.PrepareLog(logPath, logName)
	defer f2.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backend2 := logging.NewLogBackend(f2, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)

	backend2Formatter := logging.NewBackendFormatter(backend2, bchainlibs.LogFormatPimp)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")

	// Only errors and more severe messages should be sent to backend1
	backend2Leveled := logging.AddModuleLevel(backend2Formatter)
	backend2Leveled.SetLevel(logging.INFO, "")

	logging.SetBackend(backendLeveled, backend2Leveled)

	log.Info("")
	log.Info("------------------------------------------------------------------------")
	log.Info("")
	log.Info("Starting Routing process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync(targetSync, log)

	// But first let me take a selfie, in a Go lang program is getting my own IP
	me = bchainlibs.SelfieIP()
	log.Info("Good to go, my ip is " + me.String())

	// Lets prepare a address at any address at port bchainlibs.RouterPort
	ServerAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.RouterPort)
	bchainlibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
	bchainlibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the Input!
	go attendInputChannel()

	// Run the Output! The channel for communicating with the outside world!
	// The broadcast to all the MANET
	go attendOutputChannel()

	// Run the Internal channel! The direct messages to the app layer
	go attendBlockchainChannel()
	go attendMinerChannel()
	go attendRaftChannel()

	go attendResendingChannel()

	go pingInternals()

	buf := make([]byte, 1024)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		bchainlibs.CheckError(err, log)
	}

	close(input)
	close(output)
	close(blockchain)

	<-done
}
