package module

/*
	1.redis command filter
	2.error filter
 */

type filter struct {}

/*
 	command filter
  */
func (f *filter) filter() map[string]bool{
	filter := make(map[string]bool)
	// keys
	filter["KEYS"] = false
	filter["MIGIRATE"] = false
	filter["MOVE"] = false
	filter["OBJECT"] = false
	filter["DUMP"] = false
	// lists部分
	filter["BLPOP"] = false
	filter["BRPOP"] = false
	filter["BRPOPLPUSH"] = false
	filter["RPOPLPUSH"] = false
	// pub+sub
	filter["PSUBSCRIBE"] = false
	filter["PUBLISH"] = false
	filter["PUBSUBSCRIBE"] = false
	filter["SUBSCRIBE"] = false
	filter["UNSUBSCRIBE"] = false
	// transactions
	filter["DISCARD"] = false
	filter["EXEC"] = false
	filter["MULTI"] = false
	filter["UNWATCH"] = false
	filter["WATCH"] = false
	// scripting
	filter["SCRIPT"] = false
	filter["EVAL"] = false
	filter["EVALSHA"] = false
	// server
	filter["BGREWRITEAOF"] = false
	filter["BGSAVE"] = false
	filter["CLIENT"] = false
	filter["CONFIG"] = false
	filter["DBSIZE"] = false
	filter["DEBUG"] = false
	filter["FLUSHALL"] = false
	filter["FLUSHDB"] = false
	filter["LASTSAVE"] = false
	filter["LATENCY"] = false
	filter["MONITOR"] = false
	filter["PSYNC"] = false
	filter["REPLCONF"] = false
	filter["RESTORE"] = false
	filter["SAVE"] = false
	filter["SHUTDOWN"] = false
	filter["SLAVEOF"] = false
	filter["SYNC"] = false
	filter["TIME"] = false
	// slot
	filter["SLOTSCHECK"] = false
	filter["SLOTSDEL"] = false
	filter["SLOTSINFO"] = false
	filter["SLOTSMGRTONE"] = false
	filter["SLOTSMGRTSLOT"] = false
	filter["SLOTSMGRTTAGONE"] = false
	filter["SLOTSMGRTTAGSLOT"] = false
	// cluster
	filter["READONLY"] = false
	filter["READWRITE"] = false
	return filter
}
