import { rtdb, ref, onChildChanged, onValue, remove } from './firebaseconfig.js';
import express from 'express';

const app = express();
const PORT = process.env.PORT || 3000;

// Simple tracking
let processedBookings = new Set();
let totalProcessed = 0;

// Environment config
const LARAVEL_API_URL = process.env.LARAVEL_API_URL || 'https://api.multinetworkcatv.com/api/insert-booking';
const isDev = process.env.NODE_ENV !== 'production';

console.log(`🔥 Starting Firebase Monitor`);
console.log(`📡 API: ${LARAVEL_API_URL}`);
console.log(`🔧 Mode: ${isDev ? 'DEV' : 'PROD'}`);

// Basic health endpoint
app.get('/', (req, res) => {
  res.json({ 
    status: 'active',
    processed: totalProcessed,
    uptime: Math.floor(process.uptime() / 60) + 'm'
  });
});

app.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
});

// Core function: Check if booking should be processed
function shouldProcess(booking) {
  if (!booking?.bookingId) return false;
  if (processedBookings.has(booking.bookingId)) return false;
  
  const targetStatuses = ['Cancelled', 'Complete', 'Completed'];
  return targetStatuses.includes(booking.booking_status);
}

// Core function: Send to Laravel and delete
async function processBooking(booking, firebaseKey) {
  try {
    processedBookings.add(booking.bookingId);
    
    console.log(`📤 Processing: ${booking.bookingId} (${booking.booking_status})`);
    
    const response = await fetch(LARAVEL_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        assignedRider: booking.assignedRider || null,
        auto_cancel_at: booking.auto_cancel_at || null,
        booked_at: booking.booked_at || null,
        booked_at_timestamp: booking.booked_at_timestamp || null,
        bookingId: booking.bookingId,
        booking_status: booking.booking_status,
        plate_number: booking.plate_number || null,
        destinationCoordinates: booking.destinationCoordinates || null,
        expires_at: booking.expires_at || null,
        fare: booking.fare || 0,
        luggageCount: booking.luggageCount || 0,
        numberofPassengers: booking.numberofPassengers || "1",
        passengerId: booking.passengerId || null,
        passengerName: booking.passengerName || null,
        paymentMethod: booking.paymentMethod || "Cash",
        pickupCoordinates: booking.pickupCoordinates || null,
        ratings: booking.ratings || null
      }),
      signal: AbortSignal.timeout(30000) // 30s timeout
    });

    if (response.ok) {
      console.log(`✅ Sent: ${booking.bookingId}`);
      totalProcessed++;
      
      // Delete from Firebase (only in production)
      if (!isDev) {
        await remove(ref(rtdb, `Bookings/${firebaseKey}`));
        console.log(`🗑️ Deleted: ${booking.bookingId}`);
      }
    } else {
      throw new Error(`API error: ${response.status}`);
    }
    
  } catch (error) {
    console.error(`❌ Failed: ${booking.bookingId} - ${error.message}`);
    processedBookings.delete(booking.bookingId); // Allow retry
  }
}

// Firebase listeners
const bookingsRef = ref(rtdb, 'Bookings');

// Listen for changes
onChildChanged(bookingsRef, async (snapshot) => {
  const booking = snapshot.val();
  const key = snapshot.key;
  
  console.log(`🔄 Change detected: ${key} - ${booking?.booking_status}`);
  
  if (shouldProcess(booking)) {
    console.log(`🚀 Processing: ${booking.bookingId}`);
    await processBooking(booking, key);
  }
});

// Initial scan on startup
onValue(bookingsRef, async (snapshot) => {
  const data = snapshot.val();
  if (!data) return;
  
  console.log(`📊 Initial scan: ${Object.keys(data).length} bookings`);
  
  for (const [key, booking] of Object.entries(data)) {
    if (shouldProcess(booking)) {
      await processBooking(booking, key);
    }
  }
}, { onlyOnce: true });

// Aggressive keep-alive for Render
function aggressiveKeepAlive() {
  const interval = 4 * 60 * 1000; // Every 4 minutes
  
  setInterval(async () => {
    try {
      // Self-ping
      await fetch(`http://localhost:${PORT}/`, { 
        method: 'GET',
        timeout: 5000 
      });
      
      // External ping (optional - uncomment if you have a monitoring service)
      // await fetch('https://your-app-name.onrender.com/', { timeout: 5000 });
      
      console.log(`💓 Keep-alive: ${new Date().toISOString().slice(11, 19)} | Processed: ${totalProcessed}`);
      
    } catch (error) {
      console.log(`💓 Keep-alive ping failed (normal): ${error.message}`);
    }
  }, interval);
}

// Memory cleanup
setInterval(() => {
  if (processedBookings.size > 5000) {
    const oldSize = processedBookings.size;
    processedBookings = new Set([...processedBookings].slice(-2500));
    console.log(`🧹 Cleaned cache: ${oldSize} → ${processedBookings.size}`);
  }
}, 30 * 60 * 1000); // Every 30 minutes

// Start keep-alive
aggressiveKeepAlive();

// Handle shutdown
process.on('SIGTERM', () => {
  console.log('🛑 Shutting down...');
  process.exit(0);
});

console.log('✅ Firebase Monitor Active - Watching for Cancelled/Complete bookings');