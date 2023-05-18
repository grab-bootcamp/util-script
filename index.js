require('dotenv').config()
const { default: axios } = require('axios');
const mysql = require('mysql2/promise');
const { parse } = require('path');

const database = {
    db: null,

    async init() {
        this.db = await mysql.createConnection(process.env.DB_URI);
    },

    async query(queryString, params) {
        const [rows] = await this.db.execute(queryString, params);
        return rows;
    }
}

const doGetFireRisk = async (payload) => {
    const { data } = await axios.get(
        process.env.PREDICTION_APP_URL +
        '/predict?'
        + Object.entries(payload).map(([key, value]) => `${key}=${value}`).join('&')
    )
    return parseFloat(data);
}

const doUpdate = async (forestId, createdAt, fireRisk) => {
    await database.query(`
        UPDATE 
            statistic 
        SET 
            m_fire_risk = ?
        WHERE
            m_forest_id = ? AND m_created_at = ?
    `, [fireRisk, forestId, createdAt])
}

const doQuery = async (forestId, cursor) => {
    const payload = [forestId];

    if (cursor) {
        payload.push(cursor, 1);
    }

    // page size
    payload.push(100);

    const rows = await database.query(`
        SELECT 
            m_temperature, m_humidity,
            m_wind_speed, m_rain_fall,
            m_FFMC, m_DMC, m_DC, 
            m_ISI, m_BUI, m_FWI,
            m_forest_id, m_created_at
        FROM
            statistic
        WHERE
            m_forest_id = ?
            ${cursor ? 'AND m_created_at > ?' : ''}
            AND m_fire_risk IS NULL
        ORDER BY
            m_created_at ASC
        LIMIT ?${cursor ? ', ?' : ''}
    `, payload);

    return rows;
}

(async () => {
    // create the connection to database
    await database.init();

    let cursor = null;
    do {
        console.log('cursor: ', cursor);
        const data = await doQuery(2, cursor);
        console.log('data: ', data.length);
        await Promise.all(data.map(async (row) => {
            const payload = {
                temp: row.m_temperature,
                RH: row.m_humidity,
                wind: row.m_wind_speed,
                rain: row.m_rain_fall,
                FFMC: row.m_FFMC,
                DMC: row.m_DMC,
                DC: row.m_DC,
                ISI: row.m_ISI,
                BUI: row.m_BUI,
                FWI: row.m_FWI
            }

            const fireRisk = await doGetFireRisk(payload);
            await doUpdate(row.m_forest_id, row.m_created_at, Math.max(1, Math.min(99, Math.round(fireRisk * 100))));
        }))
        cursor = data[data.length - 1]?.m_created_at;
    } while (cursor);

})()