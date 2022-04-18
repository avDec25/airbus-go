package entry

type SerializationSchema string

const (
	Avro  SerializationSchema = "AVRO"
	Json                      = "JSON"
	Plain                     = "PLAIN"
)
