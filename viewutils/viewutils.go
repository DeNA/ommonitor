package viewutils

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tidwall/pretty"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"open-match.dev/open-match/pkg/pb"
)

// https://github.com/brianpursley/kubernetes/blob/3685e5e5a9c783f47a4a2d063d8abe48d6eb5702/staging/src/k8s.io/apimachinery/pkg/util/duration/duration.go#L48C1-L93C2
func HumanDuration(d time.Duration) string {
	// Allow deviation no more than 2 seconds(excluded) to tolerate machine time
	// inconsistence, it can be considered as almost now.
	if seconds := int(d.Seconds()); seconds < -1 {
		return "<invalid>"
	} else if seconds < 0 {
		return "0s"
	} else if seconds < 60*2 {
		return fmt.Sprintf("%ds", seconds)
	}
	minutes := int(d / time.Minute)
	if minutes < 10 {
		s := int(d/time.Second) % 60
		if s == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, s)
	} else if minutes < 60*3 {
		return fmt.Sprintf("%dm", minutes)
	}
	hours := int(d / time.Hour)
	if hours < 8 {
		m := int(d/time.Minute) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh%dm", hours, m)
	} else if hours < 48 {
		return fmt.Sprintf("%dh", hours)
	} else if hours < 24*8 {
		h := hours % 24
		if h == 0 {
			return fmt.Sprintf("%dd", hours/24)
		}
		return fmt.Sprintf("%dd%dh", hours/24, h)
	} else if hours < 24*365*2 {
		return fmt.Sprintf("%dd", hours/24)
	} else if hours < 24*365*8 {
		dy := int(hours/24) % 365
		if dy == 0 {
			return fmt.Sprintf("%dy", hours/24/365)
		}
		return fmt.Sprintf("%dy%dd", hours/24/365, dy)
	}
	return fmt.Sprintf("%dy", int(hours/24/365))
}

func HumanTime(t time.Time) string {
	loc := time.Now().Location()
	return t.In(loc).Format("2006-01-02 15:04:05.000")
}

func PrettyJSON(m proto.Message) string {
	marshal, err := protojson.Marshal(m)
	if err != nil {
		return "<marshal error>"
	}
	return string(pretty.Pretty(marshal))
}

func ExtensionsToStrMap(exts map[string]*anypb.Any) map[string]string {
	if exts == nil {
		return map[string]string{}
	}
	data := make(map[string]string)
	for k, v := range exts {
		b, _ := protojson.Marshal(v)
		str := string(b)
		m, _ := v.UnmarshalNew()
		switch mm := m.(type) {
		case *wrapperspb.StringValue:
			str = mm.GetValue()
		case *wrapperspb.BoolValue:
			str = fmt.Sprintf("%v", mm)
		case *wrapperspb.DoubleValue:
			str = fmt.Sprintf("%v", mm.Value)
		case *wrapperspb.FloatValue:
			str = fmt.Sprintf("%v", mm.Value)
		case *wrapperspb.Int32Value:
			str = fmt.Sprintf("%v", mm.Value)
		case *wrapperspb.Int64Value:
			str = fmt.Sprintf("%v", mm.Value)
		case *wrapperspb.UInt32Value:
			str = fmt.Sprintf("%v", mm.Value)
		case *wrapperspb.UInt64Value:
			str = fmt.Sprintf("%v", mm.Value)
		}
		data[k] = str
	}
	return data
}

func AssignmentToJSON(as *pb.Assignment) string {
	if as == nil {
		return "null"
	}
	b, err := json.Marshal(map[string]interface{}{
		"connection": as.Connection,
		"extensions": ExtensionsToStrMap(as.Extensions),
	})
	if err != nil {
		return "<json marshal error>"
	}
	return string(pretty.Pretty(b))
}

func ExtensionsToJSON(exts map[string]*anypb.Any) string {
	b, err := json.Marshal(ExtensionsToStrMap(exts))
	if err != nil {
		return "<json marshal error>"
	}
	return string(pretty.Pretty(b))
}
