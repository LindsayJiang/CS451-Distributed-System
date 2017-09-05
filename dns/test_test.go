package dns

import "testing"
//import "strconv"
import "time"
import (
	//"fmt"
	//"net/rpc"
	//"net"
	//"log"
	//"net/http"
	//"raft"
	//"dns"

)
//import "math/rand"
//import "log"
//import "strings"
//import "sync/atomic"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second



func TestWeb(t *testing.T){
	StartDNS()
	time.Sleep(100*time.Second)
}
