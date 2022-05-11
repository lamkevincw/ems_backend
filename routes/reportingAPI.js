var express = require("express");
var app = express();
var AWS = require('aws-sdk');
var Papa = require("papaparse");
var moment = require("moment");

AWS.config.loadFromPath("./config.json");

var s3 = new AWS.S3();

const site_metadata = {
    "Kerrobert": {
        "quantifiers": ["Above_Ground", "Q01", "Q03", "Q06", "Q07", "Q08", "Q10",
            "Q11", "Q12", "Q13", "Q14", "Q15", "Q17"],
        "voltage_threshold": 10,
    },
    "Hoosier": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09", "Q10",
            "Q11", "Q12", "Q13", "Q14"],
        "voltage_threshold": 10
    },
    "WR2": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09", "Q10",
            "Q11", "Q12", "Q13", "Q14"],
        "voltage_threshold": 10
    },
    "Annaheim": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05"],
        "voltage_threshold": 10
    },
    "Stony_Plains": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05"],
        "voltage_threshold": 8.1
    },
    "Meadow_Lake": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08"],
        "voltage_threshold": 8.1
    },
    "Gull_Lake": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07"],
        "voltage_threshold": 8.1
    },
    "Arrowwood": {
        "quantifiers": ["AR01", "AR02", "AR03", "AR04", "AR05"],
        "voltage_threshold": 8.1
    },
    "exsitu_NSZD_Kerrobert_v2": {
        "quantifiers": ["Q08"],
        "voltage_threshold": 10
    },
    "Drumheller": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05"],
        "voltage_threshold": 9
    },
    "Cremona": {
        "quantifiers": ["Q01", "Q02", "Q03", "Q04", "Q05"],
        "voltage_threshold": 9
    },
    "P_33rd": {
        "quantifiers": ["Q02"],
        "voltage_threshold": 9
    }
}
const sites = [
    {
        "version": "GroundPollutionSensor",
        "fileType": "up",
        "name": "Arrowwood"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Annaheim"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Gull_Lake"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Hoosier"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Kerrobert"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Meadow_Lake"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Stony_Plains"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "WR2"
    },
    {
        "version": "Quantifier-3_0",
        "fileType": "up",
        "name": "exsitu_NSZD_Kerrobert_v2"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Drumheller"
    },
    {
        "version": "Quantifier",
        "fileType": "up",
        "name": "Cremona"
    },
    {
        "version": "Quantifier-3_0",
        "fileType": "up",
        "name": "P_33rd"
    }
];
var latestFiles = [];

const currentDate = (new Date()).toLocaleDateString("en-US", { timeZone: "America/Regina" });

async function getLatestFilenames(site) {
    await s3.listObjects({
        Bucket: 'ems-sensor-data',
        Delimiter: '/',
        Prefix: site.version + "/" + site.name + "/" + site.fileType + "/"
    }, function (err, data) {
        if (err) throw err;
        // console.log(data);
        var objects = data.Contents;
        // console.log(objects);
        var objectKeys = objects.map(o => (
            o.Key
        ));
        // console.log(objectKeys);
        // console.log(site.name + ": " + objects.length);

        var latest = "";
        var lastModified = moment(0);
        for (var i = 0; i < objects.length; i++) {
            // console.log(moment(objects[i].LastModified).format('ddd MMM DD YYYY HH:mm:ss ZZ'));
            if (moment(objects[i].LastModified).utcOffset('-0600') >= lastModified
                && objects[i].Key.substring(objects[i].Key.length - 3) === "csv") {
                latest = objects[i].Key;
                lastModified = moment(objects[i].LastModified).utcOffset('-0600');
                // latestFiles.push(latest);
            }
            // console.log(latest);
        }
        latestFiles = latest;

        // const url = s3.getSignedUrl("getObject", {
        //     Bucket: "ems-sensor-data",
        //     Key: "GroundPollutionSensor/Arrowwood/up/Arrowwood_server_2021_December.csv"
        // })
    }).promise();
    return latestFiles;
}

async function readS3csv(keyName) {
    var file = s3.getObject({
        Bucket: "ems-sensor-data",
        Key: keyName
    }).createReadStream();

    return new Promise((resolve, reject) => {
        Papa.parse(file, {
            header: true,
            complete: function (results) {
                // console.log(results.data[0]);
                // console.log(keyName.split("/")[0]);
                var report = setupData(results.data);
                // console.log(report);
                // return "a";
                // if (siteResults.length === 9) {
                //     // console.log(siteResults);
                //     // res.send(siteResults);
                // }
                resolve(report);
            },
            error: function (err) {
                console.log("Papa: " + err);
                reject(err);
            }
        });
    });
}

function setupData(raw) {
    // Determine which data version is being used for current dataset
    var dataVersion;
    for (var i = 0; i < sites.length; i++) {
        if (raw[0].site === sites[i].name) {
            dataVersion = sites[i].version;
        }
    }
    // console.log(dataVersion);

    var quantifiers = [];
    var qName = "";
    switch (dataVersion) {
        case "GroundPollutionSensor":
        case "Quantifier":
            qName = "name";
            break;
        case "Quantifier-3_0":
            qName = "quantifier";
            break;
    }
    for (var i = 0; i < raw.length; i++) {
        // Find unique quantifier names
        if (quantifiers.indexOf(raw[i][qName]) === -1) {
            quantifiers.push(raw[i][qName]);
        }

    }
    quantifiers = quantifiers.filter(function (element) {
        return element !== undefined;
    }).sort();



    var quantifierReport = {};
    var lastQRow = {};
    for (var q = 0; q < quantifiers.length; q++) {
        quantifierReport[quantifiers[q]] = {
            "q_reporting": "N/A",
            "q_last_reported": "N/A",
            "q_voltage": "N/A",
            "q_voltage_low": "N/A",
            "q_sensor1": "N/A",
            "q_sensor2": "N/A",
            "q_sensor3": "N/A",
            "q_sensor1_0s": "N/A",
            "q_sensor2_0s": "N/A",
            "q_sensor3_0s": "N/A",
            "q_sensor1_too_small": "N/A",
            "q_sensor2_too_small": "N/A",
            "q_sensor3_too_small": "N/A",
        };



        switch (dataVersion) {
            case "GroundPollutionSensor":
            case "Quantifier":
                // Determine the last reported row for each quantifier and sensor
                lastQRow[quantifiers[q]] = "N/A";
                for (var i = raw.length - 1; i > 0; i--) {
                    if (raw[i][qName] === quantifiers[q] && lastQRow[quantifiers[q]] === "N/A") {
                        lastQRow[quantifiers[q]] = i;
                    }
                }
                

                var reportedDate = new Date(parseInt(raw[lastQRow[quantifiers[q]]].timestamp));
                var lastVoltage = parseFloat(raw[lastQRow[quantifiers[q]]].battery_voltage);
                // console.log(raw[0].site + " " + quantifiers[q]+ " " + reportedDate + " " + lastVoltage + "/" + site_metadata[raw[0].site].voltage_threshold);
                // console.log(Math.abs(parseInt(raw[lastQRow[quantifiers[q]]].timestamp) - (new Date()).getTime()) / (60 * 60 * 1000));
                var hoursBetweenDates = Math.abs(parseInt(raw[lastQRow[quantifiers[q]]].timestamp) - (new Date()).getTime()) / (60 * 60 * 1000);
                // console.log(reportedDate.toLocaleDateString() + " " + currentDate);
                quantifierReport[quantifiers[q]].q_reporting = (hoursBetweenDates < 24);
                quantifierReport[quantifiers[q]].q_last_reported = reportedDate.toLocaleDateString("en-US", { timeZone: "America/Regina" }) + ", " + reportedDate.toLocaleTimeString("en-US", { timeZone: "America/Regina" });
                quantifierReport[quantifiers[q]].q_voltage = lastVoltage;
                quantifierReport[quantifiers[q]].q_voltage_low = (lastVoltage < site_metadata[raw[0].site].voltage_threshold);
                // sensor1_data_received = is_active_board1.main,
                // sensor2_data_received = is_active_board2.main,
                // sensor3_data_received = is_active_board3.main,
                quantifierReport[quantifiers[q]].q_sensor1 = (raw[lastQRow[quantifiers[q]]]["is_active_board1.main"] === "true");
                quantifierReport[quantifiers[q]].q_sensor2 = (raw[lastQRow[quantifiers[q]]]["is_active_board2.main"] === "true");
                quantifierReport[quantifiers[q]].q_sensor3 = (raw[lastQRow[quantifiers[q]]]["is_active_board3.main"] === "true");
                // sensor1_raw_ref = board1.board1_detector_raw1,
                // sensor1_raw_ch4 = board1.board1_detector_raw2,
                // sensor1_raw_phc = board1.board1_detector_raw3,
                // sensor1_raw_co2 = board1.board1_detector_raw4,
                quantifierReport[quantifiers[q]].q_sensor1_0s = (parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw1"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw2"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw3"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw4"]) == 0);
                quantifierReport[quantifiers[q]].q_sensor2_0s = (parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw1"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw2"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw3"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw4"]) == 0);
                quantifierReport[quantifiers[q]].q_sensor3_0s = (parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw1"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw2"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw3"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw4"]) == 0);
                quantifierReport[quantifiers[q]].q_sensor1_too_small = (parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw1"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw2"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw3"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board1.board1_detector_raw4"]) < 10000);
                quantifierReport[quantifiers[q]].q_sensor2_too_small = (parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw1"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw2"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw3"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board2.board2_detector_raw4"]) < 10000);
                quantifierReport[quantifiers[q]].q_sensor3_too_small = (parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw1"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw2"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw3"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]]]["board3.board3_detector_raw4"]) < 10000);
                // console.log(quantifierReport);
                break;
            case "Quantifier-3_0":
                // Determine the last reported row for each quantifier and sensor
                lastQRow[quantifiers[q]] = ["N/A", "N/A", "N/A", "N/A"];
                for (var i = raw.length - 1; i > 0; i--) {
                    for (var j = 0; j < 4; j++) {
                        if (raw[i][qName] === quantifiers[q]
                            && lastQRow[quantifiers[q]][j] === "N/A"
                            && raw[i].board === j + "") {
                            lastQRow[quantifiers[q]][j] = i;
                        }
                    }
                }
                // console.log(lastQRow);
                var reportedDate = new Date(parseInt(raw[lastQRow[quantifiers[q]][0]].timestamp));
                var lastVoltage = parseFloat(raw[lastQRow[quantifiers[q]][0]].battery_voltage);

                quantifierReport[quantifiers[q]].q_reporting = (reportedDate.toLocaleDateString("en-US", { timeZone: "America/Regina" }) === currentDate);
                quantifierReport[quantifiers[q]].q_last_reported = reportedDate.toLocaleDateString("en-US", { timeZone: "America/Regina" }) + ", " + reportedDate.toLocaleTimeString("en-US", { timeZone: "America/Regina" });
                quantifierReport[quantifiers[q]].q_voltage = lastVoltage;
                quantifierReport[quantifiers[q]].q_voltage_low = (true);
                quantifierReport[quantifiers[q]].q_sensor1 = (raw[lastQRow[quantifiers[q]][1]]["detector_raw1"] !== "");
                quantifierReport[quantifiers[q]].q_sensor2 = (raw[lastQRow[quantifiers[q]][2]]["detector_raw1"] !== "");
                // quantifierReport[quantifiers[q]].q_sensor3 = (raw[lastQRow[quantifiers[q]][3]]["detector_raw1"] !== "");
                quantifierReport[quantifiers[q]].q_sensor1_0s = (parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw1"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw2"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw3"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw4"]) == 0);
                quantifierReport[quantifiers[q]].q_sensor2_0s = (parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw1"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw2"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw3"]) == 0 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw4"]) == 0);
                // quantifierReport[quantifiers[q]].q_sensor3_0s = (parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw1"]) == 0 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw2"]) == 0 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw3"]) == 0 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw4"]) == 0);
                quantifierReport[quantifiers[q]].q_sensor1_too_small = (parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw1"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw2"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw3"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][1]]["detector_raw4"]) < 10000);
                quantifierReport[quantifiers[q]].q_sensor2_too_small = (parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw1"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw2"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw3"]) < 10000 &&
                    parseFloat(raw[lastQRow[quantifiers[q]][2]]["detector_raw4"]) < 10000);
                // quantifierReport[quantifiers[q]].q_sensor3_too_small = (parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw1"]) < 10000 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw2"]) < 10000 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw3"]) < 10000 &&
                //     parseFloat(raw[lastQRow[quantifiers[q]][3]]["detector_raw4"]) < 10000);

                break;
        }
    }
    // console.log(quantifierReport);

    return ({
        "siteName": raw[0].site,
        "quantifiers": quantifiers,
        "report": quantifierReport
    });
}

app.get("/", function (req, res, next) {
    for (var i = 0; i < sites.length; i++) {
        if (sites[i].name === req.query.id) {
            // res.send(sites[i].name);
            getLatestFilenames(sites[i])
                .then((value) => {
                    // res.send(value);
                    // console.log(value);
                    readS3csv(value)
                        .then((results) => {
                            // console.log(JSON.stringify(results));
                            res.send(JSON.stringify(results));
                        })
                        .catch(console.log);

                    // console.log(results);
                    // res.send("id: " + req.query.id);
                })
                .catch(console.log);
        }
    }
});

module.exports = app;