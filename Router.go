package main

import (
	"os"
	"net"
	"time"
	"strconv"
    "encoding/json"

    "github.com/op/go-logging"
    "github.com/chepeftw/treesiplibs"
	"github.com/chepeftw/bchainlibs"
)


// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("router")

var me net.IP = net.ParseIP(bchainlibs.LocalhostAddr)

// +++++++++ Routing Protocol
var forwarded map[string]bool = make(map[string]bool)
var packets map[string]bchainlibs.Packet = make(map[string]bchainlibs.Packet)

// +++++++++ Channels
var input = make(chan string)
var output = make(chan string)
var blockchain = make(chan string)
var miner = make(chan string)
var resending = make(chan int)
var done = make(chan bool)


func sendMessage(payload bchainlibs.Packet) {
	bchainlibs.SendGeneric( output, payload, log )
	log.Debug("Sending Packet with TID " + payload.TID + " to channel output")

	go func() {
		time.Sleep( time.Second * time.Duration( 1 ) )
		log.Debug("RE attendResendingChannel => 1" )
		resending <- 1
	}()
}

func sendBlockchain(payload bchainlibs.Packet) {
	bchainlibs.SendGeneric( blockchain, payload, log )
	log.Debug("Sending Packet with TID " + payload.TID + " to channel blockchain")
}

func sendMiner(payload bchainlibs.Packet) {
	bchainlibs.SendGeneric( miner, payload, log )
	log.Debug("Sending Packet with TID " + payload.TID + " to channel miner")
}


// Function that handles the output channel
func attendOutputChannel() {
	log.Debug("Starting output channel")
	bchainlibs.SendToNetwork( bchainlibs.BroadcastAddr, bchainlibs.RouterPort, output, true, log , me )
}

func attendBlockchainChannel() {
	log.Debug("Starting blockchain channel")
	bchainlibs.SendToNetwork( me.String(), bchainlibs.BlockCPort, blockchain, false, log, me )
}

func attendMinerChannel() {
	log.Debug("Starting miner channel")
	bchainlibs.SendToNetwork( me.String(), bchainlibs.MinerPort, miner, false, log, me )
}

func attendResendingChannel() {
	log.Debug("Starting resending channel")
	for {
		_, more := <-resending
		if more {
			missingAck := 0
			for v, k := range forwarded {
				if !k {
					missingAck++
					log.Debug("Packet " + v + " not acked yet!")
					sendMessage( packets[v] )
				}
			}

			if missingAck > 0 {
				go func() {
					time.Sleep( time.Second * time.Duration( 1 ) )
					log.Debug("RE attendResendingChannel => 1" )
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
		tid := payload.TID

		switch payload.Type {

		case bchainlibs.InternalUBlockType:
			log.Debug("Receiving InternalUBlockType Packet")
			//if eqIp( me, source ) {
			payload.Type = bchainlibs.UBlockType
			forwarded["u"+tid] = false
			packets["u"+tid] = payload
			sendMessage( payload )
			log.Info("U_BLOCK_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + tid)
			//}
		break

		case bchainlibs.InternalVBlockType:
			log.Debug("Receiving InternalVBlockType Packet")
			//if eqIp( me, source ) {
			payload.Type = bchainlibs.VBlockType
			forwarded["v"+tid] = false
			packets["v"+tid] = payload
			sendBlockchain( payload )
			sendMessage( payload )
			log.Info("V_BLOCK_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + tid)
			//}
		break

		case bchainlibs.UBlockType:
			if _, ok := forwarded[ "u"+tid ]; !ok && !eqIp( me, source ) {
				log.Debug("Receiving UBlockType Packet")
				forwarded[ "u"+tid ] = true
				sendMiner( payload )
				sendMessage( payload )
				log.Info("U_BLOCK_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + tid)
			} else if !forwarded[ "u"+tid ] {
				forwarded[ "u"+tid ] = true
			}
		break

		case bchainlibs.VBlockType:
			if _, ok := forwarded[ "v"+tid ]; !ok && !eqIp( me, source ) {
				log.Debug("Receiving VBlockType Packet")
				forwarded[ "v"+tid ] = true
				sendBlockchain( payload )
				sendMessage( payload )
				log.Info("V_BLOCK_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10) + "," + tid)
			} else if !forwarded[ "v"+tid ] {
				forwarded[ "v"+tid ] = true
			}
		break



		case bchainlibs.LastBlockType:
			//if eqIp( me, source ) {
			log.Debug("Receiving LastBlockType Packet")
			sendMiner( payload )
			//}
		break

		case bchainlibs.InternalQueryType:
			if _, ok := forwarded[ "q"+tid ]; !ok{
				log.Debug("Receiving InternalQueryType Packet")
				// This is the start of the query, which later can be queried in MongoDB as the minimal value of this.
				// Then compared to the maximun I can get the time it took to propagate.
				// This is fine cause there is just one query
				log.Info("QUERY_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10))
				payload.Type = bchainlibs.QueryType
				forwarded[ "q"+tid ] = false
				packets["q"+tid] = payload
				sendBlockchain( payload )
				sendMessage( payload )
			}
		break

		case bchainlibs.QueryType:
			if _, ok := forwarded[ "q"+tid ]; !ok && !eqIp( me, source ) {
				log.Debug("Receiving QueryType Packet")
				log.Info("QUERY_TIME_RECEIVED=" + strconv.FormatInt(time.Now().UnixNano(), 10))
				forwarded[ "q"+tid ] = true
				sendBlockchain( payload )
				sendMessage( payload )
			} else if !forwarded[ "q"+tid ] {
				forwarded[ "q"+tid ] = true
			}
		break

		case bchainlibs.InternalPong:
			log.Info("Receving PONG = " + tid)
		break

		}

	} else {
	    log.Debug("closing channel")
	    done <- true
	    return
	}

    }
}

func eqIp( a net.IP, b net.IP ) bool {
    return treesiplibs.CompareIPs(a, b)
}

func pingInternals() {
	time.Sleep(time.Second * time.Duration(2))

	payload := bchainlibs.AssemblePing( me )
	sendBlockchain( payload )

	time.Sleep(time.Second * time.Duration(1))

	payload = bchainlibs.AssemblePing( me )
	sendMiner( payload )
}


func main() {

    confPath := "/app/conf.yml"
    if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
    }
    var c bchainlibs.Conf
    c.GetConf( confPath )

    targetSync := c.TargetSync
	logPath := c.LogPath

    // Logger configuration
	f := bchainlibs.PrepareLog( logPath, "router" )
	defer f.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend( backendLeveled )

    log.Info("")
    log.Info("------------------------------------------------------------------------")
    log.Info("")
    log.Info("Starting Routing process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync( targetSync, log )

    // But first let me take a selfie, in a Go lang program is getting my own IP
    me = treesiplibs.SelfieIP()
    log.Info("Good to go, my ip is " + me.String())

    // Lets prepare a address at any address at port bchainlibs.RouterPort
    ServerAddr,err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.RouterPort)
    treesiplibs.CheckError(err, log)

    // Now listen at selected port
    ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
    treesiplibs.CheckError(err, log)
    defer ServerConn.Close()

    // Run the Input!
    go attendInputChannel()
    // Run the Output! The channel for communicating with the outside world!
	// The broadcast to all the MANET
    go attendOutputChannel()
	// Run the Internal channel! The direct messages to the app layer
	go attendBlockchainChannel()
	go attendMinerChannel()

	go attendResendingChannel()

	go pingInternals()

    buf := make([]byte, 1024)

    for {
		n,_,err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		treesiplibs.CheckError(err, log)
    }

    close(input)
    close(output)
    close(blockchain)

    <-done
}
