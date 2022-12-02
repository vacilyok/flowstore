package clickhouse

import (
	"encoding/base64"
	"encoding/binary"
	cfg "flowstore/config"
	fmsgs "flowstore/pb"
	"fmt"
	"log"

	clickhouse "github.com/ClickHouse/clickhouse-go"
)

var DbConn *clickhouse.Conn

// Create connection to database
func Connection() (*clickhouse.Conn, error) {
	conn_str := fmt.Sprintf("%s:%d", cfg.Config.ClickhouseHost, cfg.Config.ClickhousePort)
	conn := clickhouse.NewConn(conn_str, clickhouse.NewHttpTransport())
	err := conn.Ping()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Create database and tables if not exists
func DbInit() error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.Config.ClickhouseDBName)
	clquery := clickhouse.NewQuery(query)
	err := clquery.Exec(DbConn)
	if err != nil {
		log.Println("Create database with name " + cfg.Config.ClickhouseDBName + " is failed")
		return err
	}

	tableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.flow_data 
	(
		event_date Date DEFAULT toDate(toDateTime(timestamp)),
		timestamp UInt32,
		vlan_id UInt16,
		agent_ip UInt32,
		subagent_id UInt32,
		seq_num UInt32,
		rate UInt32,
		src_ip UInt32,
		dst_ip UInt32,
		src_port UInt16,
		dst_port UInt16,
		proto UInt8,
		PktLen UInt32,
		tcp_flags UInt8,
		pktraw String
	)
	ENGINE = MergeTree PARTITION BY toYYYYMMDD(toDate(toDateTime(timestamp)))
	ORDER BY (event_date,timestamp) TTL event_date + toIntervalDay(14) SETTINGS index_granularity = 8192;`, cfg.Config.ClickhouseDBName)
	tclquery := clickhouse.NewQuery(tableQuery)
	err = tclquery.Exec(DbConn)
	if err != nil {
		return err
	}
	return nil
}

// Decompose flowdata,  form clickhouse.Row data and push to channel
func Decompose(m *fmsgs.FlowMessage, rowChan chan clickhouse.Row) {
	st := m.TimeFlowStart
	vlan := uint16(m.VlanId)
	agent_ip := binary.BigEndian.Uint32(m.SamplerAddress)
	subagent_id := m.SamplerSubagentId
	SequenceNum := m.SequenceNum
	SamplingRate := m.SamplingRate
	if m.SrcAddr == nil {
		m.SrcAddr = make([]byte, 4)
	}
	SrcAddr := binary.BigEndian.Uint32(m.SrcAddr)
	if m.DstAddr == nil {
		m.DstAddr = make([]byte, 4)
	}
	DstAddr := binary.BigEndian.Uint32(m.DstAddr)
	SrcPort := uint16(m.SrcPort)
	DstPort := uint16(m.DstPort)
	Proto := uint8(m.Proto)
	pktLen := uint32(m.Bytes)
	flags := m.TCPFlags
	if m.PacketSample == nil {
		m.PacketSample = make([]byte, 0)
	}
	raw := base64.StdEncoding.EncodeToString(m.PacketSample)
	row := clickhouse.Row{st, vlan, agent_ip, subagent_id, SequenceNum, SamplingRate, SrcAddr, DstAddr, SrcPort, DstPort, Proto, pktLen, flags, raw}
	rowChan <- row
}

// Read from channel clickhouse.Row data and collect to clickhouse.Rows slice
func Collect(rowChan chan clickhouse.Row) {
	rows := make(clickhouse.Rows, 0)
	for {
		row := <-rowChan
		rows = append(rows, row)
		if len(rows) > cfg.Config.CountInsertRow {
			save(&rows)
			rows = rows[:0]
		}
	}

}

// save (MultiInsert) clickhouse.Rows into database
func save(rows *clickhouse.Rows) {
	flowTable := fmt.Sprintf("%s.flow_data", cfg.Config.ClickhouseDBName)
	query, err := clickhouse.BuildMultiInsert(flowTable, clickhouse.Columns{"timestamp", "vlan_id", "agent_ip", "subagent_id", "seq_num", "rate", "src_ip", "dst_ip", "src_port", "dst_port", "proto", "PktLen", "tcp_flags", "pktraw"},
		*rows)
	if err != nil {
		log.Println(err)
		return
	}
	query.Exec(DbConn)
}
