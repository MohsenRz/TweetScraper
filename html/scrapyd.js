JOB_STATUS = { pending: "pending", running: "running", finished: "finished" };

class Scrapyd {
  constructor() {
    this.nextJobId = 1;
    this.project = "TweetScraper";
    this.spider = "TweetScraper";
  }

  async getJobs() {
    let res = await fetch(
      `http://localhost:6800/listjobs.json?project=${this.project}`
    );
    let jobs = await res.json();
    let pending = jobs.pending;
    let running = jobs.running;
    let finished = jobs.finished;

    return {
      pending,
      running,
      finished,
    };
  }

  async deamonStatus() {
    let res = await fetch("http://localhost:6800/daemonstatus.json");
    let status = await res.json();
    return status;
  }

  async scheduleJob(data) {
    data["project"] = this.project;
    data["spider"] = this.spider;
    data["id"] = this.nextJobId;
    console.log(data);
    let res = await fetch("http://localhost:6800/schedule.json", {
      method: "POST",
      headers: {
        "Access-Control-Allow-Origin": "*",
      },
      mode: "cors",
      body: JSON.stringify(data),
    });
    console.log(res);
    return res.json();
  }
}
