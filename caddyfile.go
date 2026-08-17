package caddyturso

import (
	"os"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func parseFloatArg(d *caddyfile.Dispenser) (float64, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	f, err := strconv.ParseFloat(d.Val(), 64)
	if err != nil {
		return 0, d.Errf("turso: invalid float %q: %v", d.Val(), err)
	}
	return f, nil
}

func parseQueryDef(d *caddyfile.Dispenser) (QueryDef, error) {
	var qd QueryDef
	if !d.NextArg() {
		return qd, d.ArgErr()
	}
	qd.Name = d.Val()

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "sql":
			if !d.NextArg() {
				return qd, d.ArgErr()
			}
			qd.SQL = d.Val()
		case "sql_file":
			if !d.NextArg() {
				return qd, d.ArgErr()
			}
			data, err := os.ReadFile(d.Val())
			if err != nil {
				return qd, d.Errf("turso: read sql_file: %v", err)
			}
			qd.SQL = string(data)
		case "param":
			p, err := parseParamInline(d)
			if err != nil {
				return qd, err
			}
			qd.Params = append(qd.Params, p)
		case "output":
			o, err := parseOutputConfig(d)
			if err != nil {
				return qd, err
			}
			qd.Output = o
		case "cache":
			dur, err := parseDurationArg(d)
			if err != nil {
				return qd, err
			}
			qd.CacheTTL = int64(dur)
		case "timeout":
			dur, err := parseDurationArg(d)
			if err != nil {
				return qd, err
			}
			qd.Timeout = int64(dur)
		default:
			return qd, d.Errf("turso: unknown query option: %s", d.Val())
		}
	}
	return qd, nil
}

func parseParamInline(d *caddyfile.Dispenser) (ParamBinding, error) {
	var p ParamBinding
	if !d.NextArg() {
		return p, d.ArgErr()
	}
	p.Name = strings.TrimPrefix(d.Val(), "$")

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "from", "source":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Source = d.Val()
		case "key":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Key = d.Val()
		case "type":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Type = d.Val()
		case "default":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			v := d.Val()
			p.Default = &v
		case "min":
			f, err := parseFloatArg(d)
			if err != nil {
				return p, err
			}
			p.Min = &f
		case "max":
			f, err := parseFloatArg(d)
			if err != nil {
				return p, err
			}
			p.Max = &f
		case "cap":
			f, err := parseFloatArg(d)
			if err != nil {
				return p, err
			}
			p.Cap = &f
		case "pattern":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Pattern = d.Val()
		default:
			return p, d.Errf("turso: unknown param option: %s", d.Val())
		}
	}
	return p, nil
}

func parseOutputConfig(d *caddyfile.Dispenser) (OutputConfig, error) {
	var o OutputConfig
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "format":
			if !d.NextArg() {
				return o, d.ArgErr()
			}
			o.Format = d.Val()
		case "envelope":
			if d.NextArg() {
				o.Envelope = d.Val() == "on" || d.Val() == "true"
			} else {
				o.Envelope = true
			}
		case "alias":
			var col, alias string
			if !d.NextArg() {
				return o, d.ArgErr()
			}
			col = d.Val()
			if !d.NextArg() {
				return o, d.ArgErr()
			}
			alias = d.Val()
			if o.Aliases == nil {
				o.Aliases = make(map[string]string)
			}
			o.Aliases[col] = alias
		case "omit":
			if !d.NextArg() {
				return o, d.ArgErr()
			}
			o.Omit = append(o.Omit, d.Val())
		case "status":
			n, err := parseIntArg(d)
			if err != nil {
				return o, err
			}
			o.Status = n
		case "body":
			if !d.NextArg() {
				return o, d.ArgErr()
			}
			o.Body = d.Val()
		default:
			return o, d.Errf("turso: unknown output option: %s", d.Val())
		}
	}
	return o, nil
}

func parseQueryRoute(d *caddyfile.Dispenser) (QueryRoute, error) {
	var r QueryRoute
	args := d.RemainingArgs()
	if len(args) < 3 {
		return r, d.Errf("turso: route requires: METHOD path query_name")
	}
	r.Method = args[0]
	r.Path = args[1]
	r.QueryName = args[2]

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "require_header":
			var name, value string
			if !d.NextArg() {
				return r, d.ArgErr()
			}
			name = d.Val()
			if !d.NextArg() {
				return r, d.ArgErr()
			}
			value = d.Val()
			if r.RequireHeaders == nil {
				r.RequireHeaders = make(map[string]string)
			}
			r.RequireHeaders[name] = value
		case "rate_limit":
			n, err := parseIntArg(d)
			if err != nil {
				return r, err
			}
			if !d.NextArg() || d.Val() != "per" {
				return r, d.Errf("turso: rate_limit syntax: rate_limit <n> per <duration>")
			}
			dur, err := parseDurationArg(d)
			if err != nil {
				return r, err
			}
			r.RateLimit = &RateLimit{Requests: n, Window: int64(dur)}
		default:
			return r, d.Errf("turso: unknown route option: %s", d.Val())
		}
	}
	return r, nil
}
