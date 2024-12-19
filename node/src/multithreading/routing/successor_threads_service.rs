

// Handles cases when thread has died and all
// following ack and nack messages must be deliveded
// to a successor thread. It also handles cases when
// a successor is also dead. In this case it will
// deliver messages to the grand successor thread and so on.
pub struct SuccessorThreadsService {}
