import * as sqldb from '../sqldb'

/* @IFHELPER:FUNC deleteDev = DELETE
  FROM DEVS
*/
export function deleteDev (qPars: { DEV_ID: string }) {
  const sentence = 'DELETE FROM DEVS WHERE DEVS.DEV_ID = :DEV_ID'
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC deleteFromClient = DELETE
  PARAM CLIENT_ID: {DEVS.CLIENT_ID}
  FROM DEVS
  WHERE {DEVS.CLIENT_ID} = {:CLIENT_ID}
*/
export function deleteFromClient (qPars: { CLIENT_ID: number }) {
  const sentence = 'DELETE FROM DEVS WHERE DEVS.CLIENT_ID = :CLIENT_ID'
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC dissociateFromClient = UPDATE
  PARAM CLIENT_ID: {DEVS.CLIENT_ID}
  FROM DEVS
  CONSTFIELD CLIENT_ID = NULL
  WHERE {DEVS.CLIENT_ID} = {:CLIENT_ID}
*/
export function dissociateFromClient (qPars: { CLIENT_ID: number }) {
  const sentence = `UPDATE DEVS SET CLIENT_ID = NULL WHERE DEVS.CLIENT_ID = :CLIENT_ID`
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC updateInfo = UPDATE
  FROM DEVS
  FIELD [[IFOWNPROP {:CLIENT_ID}]] DEVS.CLIENT_ID
  FIELD [[IFOWNPROP {:DEV_TYPE}]] DEVS.DEV_TYPE
*/
export function updateInfo (qPars: { CLIENT_ID?: number, DEV_TYPE?: string, DEV_ID: string }) {
  const fields: string[] = []
  if (qPars.CLIENT_ID !== undefined) { fields.push('CLIENT_ID = :CLIENT_ID') }
  if (qPars.DEV_TYPE !== undefined) { fields.push('DEV_TYPE = :DEV_TYPE') }
  if (!fields.length) throw Error('No fields to update').HttpStatus(500).DebugInfo({ qPars })

  let sentence = `UPDATE DEVS SET ${fields.join(', ')} WHERE DEV_ID = :DEV_ID`

  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC insertIgnore = INSERT IGNORE
  FROM DEVS
  FIELD DEVS.DEV_ID
  FIELD DEVS.CLIENT_ID
*/
export function insertIgnore (qPars: { DEV_ID: string, CLIENT_ID: number }) {
  const fields: string[] = []
  fields.push('DEV_ID')
  fields.push('CLIENT_ID')

  const sentence = `INSERT IGNORE INTO DEVS (${fields.join(', ')}) VALUES (:${fields.join(', :')})`
  return sqldb.execute(sentence, qPars)
}

/* @IFHELPER:FUNC getBasicInfo = SELECT ROW
  PARAM devId: {DEVS.DEV_ID}

  FROM DEVS

  SELECT DEVS.DEV_ID
  SELECT DEVS.CLIENT_ID

  WHERE {DEVS.DEV_ID} = {:devId}
*/
export function getBasicInfo (qPars: { devId: string }) {
  let sentence = `
    SELECT
      DEVS.DEV_ID,
      DEVS.CLIENT_ID
  `
  sentence += `
    FROM
      DEVS
  `

  sentence += ` WHERE DEVS.DEV_ID = :devId `

  return sqldb.querySingle<{
    DEV_ID: string
    CLIENT_ID: number
  }>(sentence, qPars)
}

/* @IFHELPER:FUNC getDetails = SELECT ROW
  PARAM devId: {DEVS.DEV_ID}

  FROM DEVS
  LEFT JOIN (CLIENTS.CLIENT_ID = DEVS.CLIENT_ID)

  SELECT DEVS.DEV_ID
  SELECT DEVS.CLIENT_ID
  SELECT DEVS.DEV_TYPE
  SELECT CLIENTS.NAME AS CLIENT_NAME

  WHERE {DEVS.DEV_ID} = {:devId}
*/
export function getDetails (qPars: { devId: string }) {
  let sentence = `
    SELECT
      DEVS.DEV_ID,
      DEVS.CLIENT_ID,
      DEVS.DEV_TYPE,
      CLIENTS.NAME AS CLIENT_NAME
  `
  sentence += `
    FROM
      DEVS
      LEFT JOIN CLIENTS ON (CLIENTS.CLIENT_ID = DEVS.CLIENT_ID)
  `

  sentence += ` WHERE DEVS.DEV_ID = :devId `

  return sqldb.querySingle<{
    DEV_ID: string
    CLIENT_ID: number
    DEV_TYPE: string
    CLIENT_NAME: string
  }>(sentence, qPars)
}
