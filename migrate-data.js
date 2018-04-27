/*
Microsoft DEV283x (edX) module 3 assignment.
(c)2018
Author: pmvanvliet
*/
// Import modules
const path = require('path')
const mongodb = require('mongodb')
const async = require('async')
const assert = require('assert');
// Data files
const customerFile = 'm3-customer-data.json'
const addressFile = 'm3-customer-address-data.json'
// Import data files
let customers = require(path.join(__dirname, customerFile))
let addresses = require(path.join(__dirname, addressFile))

// Process data migration
customers.forEach((element, index) => {
    customers[index] = Object.assign(element, addresses[index])
})
const nofCustomers = customers.length
// Write to database
const connectStr = 'mongodb://localhost:27017/'
const dbname = 'edx-course-db'
mongodb.MongoClient.connect(connectStr, (error, database) => {
    assert.equal(null, error) // No error should be present
    const db = database.db(dbname) // To overcome MongoDB v3 use
    // Initialize set size for queries from argument params. Size = 1000 when no argument is given
    const stepSize = parseInt(process.argv[2]) || nofCustomers
    // Compose tasks
    let tasks = []
    for (let i = 0; i < nofCustomers; i = i + stepSize) {
        //console.log("From " + i + " to " + (i + stepSize - 1))
        tasks.push((callback) => {
            callback(doInsert(db, customers.slice(i, i + stepSize)))
        })
    }
    console.log("Number of queries are: " + tasks.length)
    // Start asynchronous insertion into the database (be sure MongoDB is running!)
    const begin = Date.now()
    async.parallel(tasks, (error, results) => {
        if (error) console.log("Error received in asynchronous task handler: " + error)
        // We're done
        console.log("Total processing time: " + (Date.now() - begin))
        database.close()
    })
})

function doInsert(db, array) {
    return db.collection('customers').insert(array, (error, results) => { })
}