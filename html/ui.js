class UI {
  constructor() {
    this.jobTb = document.getElementById("jobTable");
    this.tweetTb = document.getElementById("tweetTable");
  }

  async listJobs(jobs) {
    var tableBody = this.jobTb.getElementsByTagName("tbody")[0];
    console.log(jobs);
    tableBody.innerHTML = "";
    jobs.forEach((job) => {
      tableBody.innerHTML += `
            <tr class='clickable-row'>
            <td>${job.id}</td>
            <td>${job.start_time ? job.start_time.split(".")[0] : ""}</td>
            <td>${job.end_time ? job.end_time.split(".")[0] : ""}</td>
            </tr>
            `;
    });
    this.addRowHandlers(this.jobTb, this.listTweets);
  }

  async listTweets(job_id) {
    console.log(job_id);
  }
  addRowHandlers(table, listTweets) {
    var rows = table.getElementsByTagName("tr");
    for (let i = 0; i < rows.length; i++) {
      var currentRow = table.rows[i];
      var createClickHandler = function (row) {
        return function () {
          var cell = row.getElementsByTagName("td")[0];
          var id = cell.innerHTML;
          listTweets(id);
        };
      };
      currentRow.onclick = createClickHandler(currentRow);
    }
  }
}
