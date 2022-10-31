function infoSubmit () {
    var command = document.getElementById('command').value;
    var database = document.getElementById('databases').value;
    var url;
    var partition_num = "";

    let array = command.split(" ");
    let command_line = array[0];
    let director = array[1];
    var new_director = director.replaceAll("/", "%2F");
    let file_name = array[2];
    let partition = array[3]

    if (typeof file_name === 'undefined') {
        file_name = "";
    }
    if (typeof partition != 'undefined') {
        partition_num = "&partition_num=" + partition
        console.log(partition_num)
    }

    if (["mkdir", "ls"].includes(command_line)) {
        url = "?directory_path=" + new_director;
        console.log(url);
    } else {
        url = "?file_path=" + new_director;
        console.log(url);
    }

    if (["mkdir", "rm"].includes(command_line)) {
        $.ajax({
            type: 'PUT',
            url: 'http://192.168.1.68:5000/api/v1/' + command_line + url + file_name + "&db=" + database,
            success: function (response) {
                console.log(response);
                return_detail(response);
            }
        })
    } else {
        $.ajax({
            type: 'GET',
            url: 'http://192.168.1.68:5000/api/v1/' + command_line + url + file_name + "&db=" + database + partition_num,
            success: function (response) {
                console.log(response);
                return_detail(response);
            }
        })
    }
}

function return_detail(response) {
    var detail = document.getElementById("detail");
    detail.style.backgroundColor = "white";
    detail.style.display = "block";
    //console.log(typeof(response));
    let p = document.createElement('p');
    p.setAttribute('class', 'json_result');
    if (typeof(response) === 'object') {
        p.innerHTML = JSON.stringify(response, null, 4);
    } else {
        p.innerHTML = response;
    }
    detail.append(p)
}

function clearInfo() {
    document.getElementById('command').value = '';
    document.getElementById('databases').value = "mongo";
    document.getElementById('detail').innerHTML = "";
    document.getElementById('detail').style.backgroundColor = "lightgray";
}
//http://127.0.0.1:5000/api/v1/cat?file_path=%2Fuser%2Fjohn%2Ftestcsv.csv&db=firebase
//http://127.0.0.1:5000/api/v1/cat?file_path=%2Fuser%2Fjohn%2Ftestcsv.csv&db=firebase

//http://192.168.1.68:5000/api/v1/cat?file_path=%2Fuser%2Fjohntestcsv.csv&db=firebase
//http://192.168.1.68:5000/api/v1/cat?file_path=%2Fuser%2Fjohn%2Ftestcsv.csv&db=firebase

// mkdir /test2/test3
// ls /
// cat /user/john testcsv.csv
// rm /test2
// getPartitionLocations /test/john/ small_test.csv
// readPartition /user/john/ testcsv.csv 1 (start with 1)