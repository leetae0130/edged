const fs = require('fs');
const io = require('socket.io-client');
const ss = require('socket.io-stream');
var socket = io.connect('http://143.248.157.206:3333');

// flags for tracking whether device is ready to execute the command
var flags = {
  jsonFileWriteDone : false,
  execFileWriteDone : false,
  inputFileWriteDone : false,
  paramFileWriteDone : true
}

if(!process.argv[2]){
  console.log('Json file should be given');
  process.exit(1);
}

async function uploadFile(file, index, name_str){
  var stream = ss.createStream();
  ss(socket).emit(name_str, stream, {name : file, index : index});
  var json_read = fs.createReadStream(file);
  json_read.pipe(stream);
}

function uploadFiles(file_list, index_list, default_str, flag_key){
  var j = 0;
  for(let idx=0; idx < file_list.length; idx++){
    let file_name = file_list[idx];
    let file_index = index_list[idx];
    uploadFile(file_name, file_index, default_str+idx, function(){
      j++;
      if(j >= file_list.length){
        flags[flag_key] = true;
      }
    });
  }
}

var json_file = process.argv[2];
var json_index = process.argv[3];
var json_data = JSON.parse(fs.readFileSync(json_file, 'utf8'));

function main(){
  console.time('base');
  var json_send = uploadFile(json_file, json_index, 'json');
  flags['jsonFileWriteDone'] = true;
  ss(socket).on('ready', function(){
    //if(json_data.parameters){
    //  var parameter_names = json_data.parameters.file_name;
    //  var parameter_indexes = json_data.parameters.index;
    //  uploadFiles(parameter_names, parameter_indexes, 'param', 'paramFileWriteDone');
    //}
    if(json_data.exec_file){
      var exec_names = json_data.exec_file.file_name;
      var exec_index = json_data.exec_file.index;
      uploadFiles(exec_names, exec_index, 'exec', 'execFileWriteDone');
    }
    if(json_data.input_data){
      var input_index = json_data.input_data.index;
      var input_name = json_data.input_data.file_name;
      uploadFile(input_name, input_index, 'input', function(){
        flags['inputFileWriteDone'] = true;
      });
    }
  });
  ss(socket).on('result', function(data){
    console.log(data.output);
    console.timeEnd('base');
    process.exit(0);
  })
}

main();
