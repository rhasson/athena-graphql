module.exports = {
  initConnection: function(jdbc) {
    return new Promise((resolve, reject) => {
      jdbc.initialize((err) => {
        if (err) return reject(err)
        jdbc.reserve((err, connObj) => {
          if (err || !connObj) return reject(err)
          return resolve(connObj.conn)
        })
      })
    })
  },

  closeConnection: function(jdbc, conn) {
    return new Promise((resolve, reject) => {
      jdbc.release(conn, (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  },

  sqlQuery: function(conn, sql) {
    return new Promise((resolve, reject) => {
      conn.createStatement((s_err, statement) => {
        if (s_err) return reject(s_err)
        statement.executeQuery(sql, (q_err, results) => {
          if (q_err) return reject(q_err)
          results.toObjArray((e, data) => { 
            if (e) return reject (e)
            return resolve(data)
          })
        })
      })
    })
  }
}