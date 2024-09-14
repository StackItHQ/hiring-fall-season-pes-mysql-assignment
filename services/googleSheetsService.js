// services/googleSheetsService.js
const { google } = require('googleapis');
const path = require('path');
require('dotenv').config();

const sheets = google.sheets('v4');
const auth = new google.auth.GoogleAuth({
  keyFile: path.join(__dirname, '../credentials.json'), // Path to your credentials JSON file
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const spreadsheetId = '1RJ9AHDTZid-IG1PYnf-dgGlnKGtCyTveV38Uhq6WQIA'; // Replace with your Google Sheet ID

const googleSheetsService = {
  async readSheet(range) {
    const client = await auth.getClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
      auth: client
    });
    return response.data.values;
  },

  async writeSheet(range, values) {
    const client = await auth.getClient();
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      resource: {
        values
      },
      auth: client
    });
  }
};

module.exports = googleSheetsService;
