import * as xml2json from 'xml2json';
let parserC = xml2json;
import * as json2csv from 'json-2-csv';
let converter = json2csv;
import pg from 'pg';
const { Client } = pg;
import AWS from 'aws-sdk';
//const AWS = require('aws-sdk');

//Credenciales de acceso al AWS
AWS.config.update({
    region: 'us-east-1', // use appropriate region
    accessKeyId: 'AKIA2RUS6R7Z6F32DEPH', // use your access key
    secretAccessKey: 'CpH4o2h28F2PS5ZoQ66BYVwTsFMgR2G7PLymTezY' // user your secret key
})
const s3 = new AWS.S3();

const opts = {
    Bucket: 'aduanabucket' /* required */,
};

(async () => {

    try {
        //Consulta lista de archivos disponibles en el S3
        s3.listObjectsV2(opts, function(err, data) {
            if (err){
                console.log(err, err.stack); // an error occurred
            }
            else{
                //console.log(data);           // successful response

                data.Contents.map((s3ObjectInfo)=>{

                    if(s3ObjectInfo.Key.includes('.xml')){
                        var Objparams = {
                            Bucket: "aduanabucket",
                            Key: s3ObjectInfo.Key
                        };

                        s3.getObject(Objparams, function(err, data) {
                            if (err) {
                                console.log(err, err.stack);
                            }// an error occurred
                            else {
                                let jsonString = parserC.toJson(data.Body.toString());
                                let jsonData = JSON.parse(jsonString);
                                //console.log(jsonData)
                                for(var main in jsonData.Documento){
                                    let allData = jsonData.Documento[main];
                                    if(typeof allData === 'object' && allData !== null){

                                        (async () => {

                                            for(var keyContent in allData){

                                                try {
                                                    if(keyContent === 'participacion'){
                                                        for(var index in allData[keyContent]){

                                                            let par = allData[keyContent][index];
                                                            console.log(par);
                                                            // const client = new Client({
                                                            //     user: 'Sisactivos',
                                                            //     password: 'Sisactivos2023',
                                                            //     database: 'postgres',
                                                            //     port: 5432,
                                                            //     host: 'ls-f1b26516bde6362e3809bc9907a97bb101fe7e3c.cam36ft1om2h.us-east-1.rds.amazonaws.com',
                                                            //     ssl: {
                                                            //         rejectUnauthorized: false,
                                                            //     }
                                                            // });

                                                            const client = new Client({
                                                                user: 'AduanaDB',
                                                                password: 'AduanaChile',
                                                                database: 'my_database',
                                                                port: 5432,
                                                                host: 'aduana-auroraserverlessdatabase-svcteabvt753.cluster-cyorgiutggcy.us-east-1.rds.amazonaws.com',
                                                                ssl: {
                                                                    rejectUnauthorized: false,
                                                                }
                                                            });

                                                            await client.connect(err => {
                                                                if (err) {
                                                                    console.log(err);
                                                                    return;
                                                                }
                                                                client.query(`INSERT INTO public.participaciones ("nacion-id", "valor-id", "codigo-pais", nombres, direccion, comuna, nombre, "tipo-id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, [par["nacion-id"], par["valor-id"], par["codigo-pais"], par["nombres"], par["direccion"], par["comuna"], par["nombre"], par["tipo-id"]], (err, result) => {
                                                                    if (err) {
                                                                        console.log(err);
                                                                    } else {
                                                                        console.log(result);
                                                                    }
                                                                    client.end();
                                                                });
                                                            });
                                                        }
                                                    }
                                                    console.log(s3ObjectInfo.Key, keyContent);

                                                    if(allData[keyContent] !== undefined){
                                                        const csv = await converter.json2csv(allData[keyContent]);

                                                        var UploadParams = {
                                                            Bucket:  'aduanabucket',
                                                            Key: (s3ObjectInfo.Key.replace('xml',''))+'_'+keyContent+'.csv',
                                                            ContentDisposition: 'attachment',
                                                            Body: csv
                                                        };
                                                        s3.upload(UploadParams, function(err, data) {
                                                            if (err) {
                                                                console.log("Error in upload");
                                                                console.log(err)
                                                            }
                                                            if (data) {
                                                                console.log("Upload Success", data);
                                                            }
                                                        });
                                                    }
                                                } catch (err) {
                                                    console.log(err)
                                                }
                                            }
                                        })()
                                    }
                                }
                            }
                        });
                    }
                });
            }
        });

    } catch (error) {
        console.error(error.stack);
        return false;
    }
})()

//exports.handler = ;

