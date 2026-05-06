const mqtt = require("mqtt");
const admin = require("firebase-admin");
 
const express = require("express");
const app = express();

app.get("/", (req, res) => {
  res.send("Backend is running");
});

app.listen(3000, () => {
  console.log("🌐 HTTP server running on port 3000");
});

// ================= FIREBASE =================
const serviceAccount = JSON.parse(process.env.FIREBASE_KEY);
 
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://smart-exam-allotment-system-default-rtdb.asia-southeast1.firebasedatabase.app"
});
 
const db = admin.database();
 
// ================= MQTT =================
const client = mqtt.connect(
  "mqtts://0bf03b8acba14c40a9acde5f76d7d823.s1.eu.hivemq.cloud:8883",
  {
    username: "exam_system",
    password: "Exam@1234"
  }
);
 
const scanTopic     = "exam/hallA/scan";
const responseTopic = "exam/hallA/response/gate1";
 
// ================= CONNECT =================
client.on("connect", () => {
  console.log("✅ MQTT Connected");
  client.subscribe(scanTopic);
});
 
// ================= MESSAGE =================
client.on("message", async (topic, message) => {
 
  const uid = message.toString().toUpperCase().trim();
  console.log("\n📡 Scanned UID:", uid);
 
  try {
    // Read only /students — not entire DB root (faster)
    const snapshot = await db.ref("students").once("value");
    const data = snapshot.val();
 
    if (!data) {
      console.log("❌ No students in RTDB");
      client.publish(responseTopic, "INVALID");
      return;
    }
 
    let foundUser = null;
    let userPath  = null;
 
    // Search through students
    for (let key in data) {
      const user = data[key];
      if (!user || !user.uid) continue;
      const dbUID = String(user.uid).toUpperCase().trim();
      if (dbUID === uid) {
        foundUser = user;
        userPath  = `students/${key}`;
        break;
      }
    }
 
    // ================= RESULT =================
    if (foundUser && foundUser.status === "allowed") {
 
      console.log("✅ MATCH:", foundUser.name, "| PATH:", userPath);
 
      // 1. Write attendance = "present"
      //    App watcher detects this and logs to Firestore automatically
      await db.ref(userPath).update({ attendance: "present" });
      console.log("✅ Attendance written");
 
      // 2. Wait 2 seconds then CLEAR the attendance field
      //    This resets the trigger so the same student can be detected next session
      setTimeout(async () => {
        await db.ref(userPath + "/attendance").remove();
        console.log("🔄 Attendance field cleared for next session:", foundUser.name);
      }, 2000);
 
      // 3. Publish VALID response to ESP
      client.publish(responseTopic, "VALID|" +
        (foundUser.name   || "NA") + "|" +
        (foundUser.rollNo || "NA") + "|" +
        (foundUser.seat   || "NA") + "|" +
        (foundUser.branch || "NA") + "|" +
        (foundUser.room   || "NA")
      );
 
    } else {
      console.log("❌ NOT FOUND or NOT ALLOWED");
      client.publish(responseTopic, "INVALID");
    }
 
  } catch (err) {
    console.error("🔥 ERROR:", err);
  }
});