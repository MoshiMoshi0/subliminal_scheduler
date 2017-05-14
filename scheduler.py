"""Run the scheduler process."""

from ndscheduler.server import server


class SubliminalServer(server.SchedulerServer):
    pass

if __name__ == "__main__":
    SubliminalServer.run()
