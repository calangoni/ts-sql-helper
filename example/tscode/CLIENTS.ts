import * as sqldb from '../sqldb'

/* @IFHELPER:FUNC deleteRow = DELETE
  PARAM clientId: {CLIENTS.CLIENT_ID}
  FROM CLIENTS
  WHERE {CLIENTS.CLIENT_ID} = {:clientId}
*/
export function deleteRow (qPars: { clientId: number }, delChecks: {
  DEVS: true
}) {
  const sentence = 'DELETE FROM CLIENTS WHERE CLIENTS.CLIENT_ID = :clientId'
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC insert = INSERT
  FROM CLIENTS

  FIELD CLIENTS.EMAIL
  FIELD [[IFOWNPROP {:NAME}]] CLIENTS.NAME
*/
export function insert (qPars: { EMAIL: string, NAME?: string }) {
  const fields: string[] = []
  fields.push('EMAIL')
  if (qPars.NAME !== undefined) { fields.push('NAME') }

  const sentence = `INSERT INTO CLIENTS (${fields.join(', ')}) VALUES (:${fields.join(', :')})`
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC update = UPDATE
  PARAM CLIENT_ID: {CLIENTS.CLIENT_ID}

  FROM CLIENTS

  FIELD [[IFOWNPROP {:EMAIL}]] CLIENTS.EMAIL
  FIELD [[IFOWNPROP {:NAME}]] CLIENTS.NAME
*/
export function update (qPars: { CLIENT_ID: number, EMAIL?: string, NAME?: string }) {
  const fields: string[] = []
  if (qPars.EMAIL !== undefined) { fields.push('EMAIL = :EMAIL') }
  if (qPars.NAME !== undefined) { fields.push('NAME = :NAME') }
  if (!fields.length) throw Error('No fields to update').HttpStatus(500).DebugInfo({ qPars })

  let sentence = `UPDATE CLIENTS SET ${fields.join(', ')} WHERE CLIENT_ID = :CLIENT_ID`

  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC getClientInfo = SELECT ROW
  PARAM CLIENT_ID: {CLIENTS.CLIENT_ID}

  FROM CLIENTS

  SELECT CLIENTS.CLIENT_ID
  SELECT CLIENTS.NAME
  SELECT CLIENTS.EMAIL

  WHERE {CLIENTS.CLIENT_ID} = {:CLIENT_ID}
*/
export function getClientInfo (qPars: { CLIENT_ID: number }) {
  let sentence = `
    SELECT
      CLIENTS.CLIENT_ID,
      CLIENTS.NAME,
      CLIENTS.EMAIL
  `
  sentence += `
    FROM
      CLIENTS
  `

  sentence += ` WHERE CLIENTS.CLIENT_ID = :CLIENT_ID `

  return sqldb.querySingle<{
    CLIENT_ID: number
    NAME: string
    EMAIL: string
  }>(sentence, qPars)
}

/* @IFHELPER:FUNC getClientsList = SELECT LIST
  PARAM clientIds?: number[]
  PARAM+ full?: boolean

  FROM CLIENTS

  SELECT CLIENTS.CLIENT_ID
  SELECT CLIENTS.NAME
  SELECT [[IFJS admPars.full === true]] CLIENTS.EMAIL

  WHERE [[IFJS qPars.clientIds]] {CLIENTS.CLIENT_ID} IN ({:clientIds})
  WHERE [[IFJS admPars.full !== true]] {CLIENTS.NAME} <> ''

  SQLENDING [[IFJS admPars.full === true]] ORDER BY {CLIENTS.EMAIL} ASC
  SQLENDING [[IFJS admPars.full !== true]] ORDER BY {CLIENTS.NAME} ASC
*/
export function getClientsList (qPars: { clientIds?: number[] }, admPars: { full?: boolean }) {
  let sentence = `
    SELECT
      CLIENTS.CLIENT_ID,
      CLIENTS.NAME
  `
  if (admPars.full === true) { sentence += ' ,CLIENTS.EMAIL ' }
  sentence += `
    FROM
      CLIENTS
  `

  const conditions: string[] = []
  if (qPars.clientIds) { conditions.push(`CLIENTS.CLIENT_ID IN (:clientIds)`) }
  if (admPars.full !== true) { conditions.push(`CLIENTS.NAME <> ''`) }
  if (conditions.length) { sentence += ' WHERE ' + conditions.join(' AND ') }

  if (admPars.full === true) { sentence += ` ORDER BY CLIENTS.EMAIL ASC ` }
  if (admPars.full !== true) { sentence += ` ORDER BY CLIENTS.NAME ASC ` }

  return sqldb.query<{
    CLIENT_ID: number
    NAME: string
    EMAIL?: string
  }>(sentence, qPars)
}
