function onEdit(e) {
    // Check if the event object (e) is valid
    if (!e) {
      Logger.log("onEdit event object is undefined. Make sure you're editing the sheet directly.");
      return;
    }
    
    // Log the active range and sheet
    Logger.log("onEdit triggered");
    Logger.log("Active range: " + e.range.getA1Notation());
    Logger.log("Active sheet: " + e.source.getActiveSheet().getName());
  
    var sheet = e.source.getActiveSheet();
    
    // Find the last row containing data in column A (adjust if needed)
    var lastRow = sheet.getLastRow();
    
    // Define the range dynamically based on the last row with values
    var range = sheet.getRange(1, 1, lastRow,4);  // This gets all rows from A1 to Z(lastRow)
    var values = range.getValues();
  
    // Filter out empty rows (where all values in the row are empty)
    var nonEmptyValues = values.filter(row => row.some(cell => cell !== ""));
    
    // Webhook URL of your Flask server
    var url = "https://07fa-2405-201-d031-60ab-2c7e-16b9-817b-aa26.ngrok-free.app/webhook";
    
    var payload = {
      "range": "Employee!A1:D" + lastRow,
      "values": nonEmptyValues  // Sending only rows with actual data
    };
  
    var options = {
      "method": "post",
      "contentType": "application/json",
      "payload": JSON.stringify(payload)
    };
    
    // Send the HTTP POST request to your Flask webhook
    try {
      var response = UrlFetchApp.fetch(url, options);
      Logger.log("Response from webhook: " + response.getContentText());
    } catch (error) {
      Logger.log("Error sending data to webhook: " + error.message);
    }
  }
  