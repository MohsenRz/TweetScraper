const scrapyd = new Scrapyd();
const ui = new UI();

// connect to scrapyd server on DOM load
document.addEventListener("DOMContentLoaded", connect);

document.getElementById("btnSchedule").addEventListener("click", schedule);
var query = document.getElementById("queryInput");

var badge = document.querySelector(".badge");

// connect to scrapyd and get jobs
async function connect() {
  var deamonStatus = await scrapyd.deamonStatus();
  if (deamonStatus.status == "ok") {
    var jobs = await scrapyd.getJobs();
    var pending = jobs.pending;
    var running = jobs.running;
    var finished = jobs.finished;
    var jobsList = [...pending, ...running, ...finished];
    await ui.listJobs(jobsList);
    badge.textContent = "Connected";
    badge.classList.toggle("badge-danger");
    badge.classList.toggle("badge-success");
  } else {
    badge.textContent = "Not Connected";
    badge.classList.toggle("badge-success");
    badge.classList.toggle("badge-danger");
  }
}

async function schedule(e) {
  e.preventDefault();
  var res = await scrapyd.scheduleJob({ query: query.value });
  console.log(res);
}

async function showTweets(e) {
  console.log("here we are!!!");
  console.log(e);
}
