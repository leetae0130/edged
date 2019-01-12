const fs = require('fs');
const io = require('socket.io').listen(3333);
const ss = require('socket.io-stream');
const path = require('path');
const exec = require('child_process').exec;

var flags = {
  jsonFileReady : false,
  execFileReady : false,
  inputFileReady : false,
  paramFileReady : true
}
var json_data = "";

function execLogic(socket) {
  console.time('offloading');
  if(json_data.run_command){
    console.log('run');
    var childPs = exec(json_data.run_command, function(error,stdout,stderr) {
      if(error){
        console.log(error);
      }
      else{
        ss(socket).emit('result', {output:stdout});
        console.timeEnd('offloading');
        for (var key in flags){
          flags[key] = false;
        }
	flags['paramFileReady'] = true;
      }
    });
  }
}

function checkReady(){
  return (flags['jsonFileReady'] && flags['execFileReady']
        && flags['inputFileReady'] && flags['paramFileReady']);
}

function getFiles(file_names, socket, default_str, flag_key){
  var j = 0;
  for(let idx=0; idx < file_names.length; idx++){
    ss(socket).on(default_str+idx, function(stream, data){
      let file_name = path.basename(data.name);
      stream.pipe(fs.createWriteStream(file_name));
      stream.on('finish',function(){
        if(++j >= file_names.length){
          flags[flag_key] = true;
          if (checkReady()){
            console.timeEnd('downloading');
            execLogic(socket);
          }
        }
      })
    });
  }
}


io.on('connection', function(socket){
  console.log('connected');
  ss(socket).on('json', async function(stream, data){
    console.time('downloading');
    var json_name = data.name;
    await stream.pipe(fs.createWriteStream(json_name));
    stream.on('finish', function(){
      json_data = JSON.parse(fs.readFileSync(json_name));
      flags['jsonFileReady'] = true;
      //if(json_data.parameters){
      //  var parameter_names = json_data.parameters.file_name;
      //  getFiles(parameter_names, socket, 'param', 'paramFileReady');
      //}
      //else{
      //  flags['paramFileReady'] = true;
      //}
      if(json_data.input_data){
        var input_data = json_data.input_data.file_name;
        ss(socket).on('input', function(stream, data){
          let input_name = path.basename(data.name);
          stream.pipe(fs.createWriteStream(input_name))
          stream.on('finish', function(){
            flags['inputFileReady'] = true;
            if(checkReady()){
              console.timeEnd('downloading');
	      execLogic(socket);
            }
          })
        });
      }
      else{
        flags['inputFileReady'] = true;
      }
      if(json_data.exec_file){
        var exec_names = json_data.exec_file.file_name;
        getFiles(exec_names, socket, 'exec', 'execFileReady');
      }
      else{
        flags['execFileReady'] = true;
      }
      ss(socket).emit('ready');
    });
  })
})
