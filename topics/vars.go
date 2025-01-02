package topics

type checkpoint struct {
	Load string
	Save string
}

type cdcSubjects struct {
	Event string
}

// Directly export the Checkpoints and CDC variables
var Checkpoints = checkpoint{
	Load: "checkpoint.load",
	Save: "checkpoint.save",
}

var CDC = cdcSubjects{
	Event: "cdc.event",
}
