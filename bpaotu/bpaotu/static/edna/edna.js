console.log("edna js");

// they use jquery to get button clicks so we'll do the same.

$(document).ready(function() {
  var setup_file_submit = function() {
    $("#file_submit_btn").on("click", "#add", function() {
      console.log("hihihi");
    });
  };
});

// $("#file_submit_btn").click(function() {
//   console.log("submit button was clicked");
// });
