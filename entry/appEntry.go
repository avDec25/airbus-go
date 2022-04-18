package entry

type AppEntry struct {
	Id       float64                  `json:"id"`
	Name     string                   `json:"name"`
	Produces []map[string]interface{} `json:"produces"`
	Consumes []ConsumerEventEntry     `json:"consumes"`
}
