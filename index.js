'use strict'

const ChangesStream = require(process.env.CHANGES_STREAM || 'changes-stream')
const got = require('got')

const db = process.argv[2] || 'http://localhost:5984'
const secret = process.argv[3] || ''
const upstream = 'https://replicate.npmjs.com/registry'
const concurrency = 30
let since = process.argv[4] || 0

const privatePackages = {}
const erredPackages = []

let count = 0
let docCount = 0
let publicCount = 0
let privateCount = 0
let errorCount = 0
let openRequests = 0
let paused = false

process.on('exit', () => {
  console.log(JSON.stringify(privatePackages))
  console.log()
  console.log(erredPackages)
  console.log(`
privateCount: ${privateCount}
 publicCount: ${publicCount}
  errorCount: ${errorCount}
    docCount: ${docCount}
       count: ${count}
   abandoned: ${docCount - (privateCount + publicCount + errorCount)}
openRequests: ${openRequests}
`)
})

process.on('SIGINT', () => {
  process.exit(0)
})

const dbOpts = { json: true }
if (process.env.NPM_TOKEN) dbOpts.headers = { Authorization: `Bearer ${process.env.NPM_TOKEN}` }
if (secret) {
  dbOpts.query = { sharedFetchSecret: secret }
  dbOpts.query_params = { sharedFetchSecret: secret }
}
if (!isNaN(parseInt(since, 10))) {
  since = parseInt(since, 10)
  dbOpts.since = since
  count = since
}

function getAuthors (pkgUrl) {
  return got(`${db}/${pkgUrl}`, dbOpts)
    .then(res => {
      let authors = []
      if (res.body.author) authors = authors.concat(res.body.author)
      if (res.body.maintainers) authors = authors.concat(res.body.maintainers)
      return authors
    })
    .catch(err => {
      if (err.statusCode === 404) {
        return 404
      }
      console.error(err)
      return []
    })
}

got(db, dbOpts)
.then(res => {
  console.log(res.body)
  return res.body.update_seq
})
.catch(err => {
  console.error(err)
  process.exit(1)
})
.then(update_seq => {
  const changes = new ChangesStream(Object.assign({
    db: db,
    include_docs: true
  }, dbOpts))

  setInterval(() => {
    console.log(`
===== open requests: ${openRequests}
=====        paused: ${paused}
`)
    if (!paused && openRequests < 5) changes.resume()
  }, 5000)

  function checkDone () {
    if (count >= update_seq) process.exit(0)
    if ((docCount - (publicCount + privateCount + errorCount)) < Math.min(5, concurrency)) {
      changes.resume()
      paused = false
    }
  }

  changes.on('error', (err) => {
    console.error('COUCH ERROR', err)
    process.exit(1)
  })

  changes.on('readable', () => {
    let change = changes.read()
    if (!change) return;
    count++
    if (change && change.doc && change.doc.name) {
      docCount++

      if ((docCount - (publicCount + privateCount + errorCount)) >= concurrency) {
        changes.pause()
        paused = true
      }

      let pkg = change.doc.name
      let pkgUrl = String(pkg).replace('/', '%2f')
      console.log(`${count} ${pkg}`)
      openRequests++
      got(`${upstream}/${pkgUrl}`, { json: true })
        .then(res => {
          publicCount++
          console.log(`public package: ${pkg}`)
          checkDone()
          openRequests--
        })
        .catch(err => {
          if (err.statusCode === 404) {
            privateCount++
            console.log(`PRIVATE PACKAGE FOUND: ${pkg}`)
            getAuthors(pkgUrl).then(authors => {
              if (authors !== 404) privatePackages[pkg] = authors
              checkDone()
              openRequests--
            })
          } else {
            errorCount++
            console.error('>> error', err)
            erredPackages.push(pkg)
            checkDone()
            openRequests--
          }
        })
    } else {
      console.error('>> no doc or name')
      checkDone()
    }
  })
})
