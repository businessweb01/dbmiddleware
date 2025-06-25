import { rtdb, ref, onChildChanged, onValue, remove } from './firebaseconfig.js';
import express from 'express';

// ✅ Add HTTP server for Render health checks
const app = express();
const PORT = process.env.PORT || 3000;

// Simple health check endpoint
app.get('/', (req, res) => {
  res.json({ status: 'ok' });
});

// Start HTTP server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌐 HTTP server running on port ${PORT}`);
});

console.log('🔥 Service started. Monitoring Firebase bookings…');

const bookingsRef = ref(rtdb, 'Bookings');

// ✅ Environment-based configuration
const isProduction = process.env.NODE_ENV === 'production';
const LARAVEL_API_URL = 'https://api.multinetworkcatv.com/api/insert-booking'; // Production URL
// const LARAVEL_API_URL = isProduction 
//   ? 'https://api.multinetworkcatv.com/api/insert-booking'
//   : 'http://127.0.0.1:8000/api/insert-booking'; // Local Laravel URL

console.log(`🌐 Using API URL: ${LARAVEL_API_URL}`);
console.log(`🔧 Environment: ${isProduction ? 'PRODUCTION' : 'DEVELOPMENT'}`);

// Track processed bookings to avoid duplicates
const processedBookings = new Set();

// ✅ Helper function to check if booking should be processed
function shouldProcessBooking(booking) {
  if (!booking || typeof booking !== 'object') {
    return { process: false, reason: 'Invalid booking object' };
  }

  if (!booking.bookingId) {
    return { process: false, reason: 'Missing booking ID' };
  }

  if (processedBookings.has(booking.bookingId)) {
    return { process: false, reason: 'Already processed' };
  }

  // Check for both 'Cancelled', 'Complete', and 'Completed' to handle inconsistencies
  const targetStatuses = ['Cancelled', 'Complete', 'Completed'];
  if (!targetStatuses.includes(booking.booking_status)) {
    return { process: false, reason: `Status is '${booking.booking_status}', not in target statuses` };
  }

  return { process: true, reason: 'Valid for processing' };
}

// ✅ 1. Initial scan on startup
onValue(bookingsRef, async (snapshot) => {
  const data = snapshot.val();
  if (!data) {
    console.log('📭 No bookings found in initial scan');
    return;
  }

  console.log(`📊 Initial scan found ${Object.keys(data).length} bookings`);

  for (const key in data) {
    const booking = data[key];
    const { process, reason } = shouldProcessBooking(booking);

    if (!process) {
      if (reason !== 'Already processed' && reason !== `Status is '${booking?.booking_status}', not in target statuses`) {
        console.log(`⚠️ Skipping booking ${key}: ${reason}`);
      }
      continue;
    }

    console.log(`🛠 Initial scan matched: ${booking.bookingId} - ${booking.booking_status}`);
    await sendToLaravelAndDelete(booking, key);
  }
}, (error) => {
  console.error('❌ Error in initial scan:', error);
});

// ✅ 2. Real-time monitoring for changes - THIS IS THE MAIN TRIGGER
onChildChanged(bookingsRef, async (snapshot) => {
  const booking = snapshot.val();
  const key = snapshot.key;

  console.log(`🔄 Firebase UPDATE detected for key: ${key}`);
  console.log(`📋 Current booking status: ${booking?.booking_status}`);

  const { process, reason } = shouldProcessBooking(booking);

  if (!process) {
    console.log(`⏭️ Skipping booking ${key}: ${reason}`);
    return;
  }

  console.log(`🚀 STATUS CHANGE TRIGGER: ${booking.bookingId} updated to '${booking.booking_status}' - Processing now...`);
  await sendToLaravelAndDelete(booking, key);
}, (error) => {
  console.error('❌ Error in real-time monitoring:', error);
});

// ✅ 3. Enhanced handler for Laravel and delete
async function sendToLaravelAndDelete(booking, firebaseKey) {
  try {
    // Mark as being processed immediately to prevent duplicates
    processedBookings.add(booking.bookingId);
    
    console.log(`📤 Processing booking ${booking.bookingId} with status '${booking.booking_status}'...`);
    
    // Clean and validate the booking data
    const cleanedBooking = {
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
      ratings:booking.ratings || null
    };

    // Log the cleaned data being sent
    console.log('📦 Sending data to Laravel:', JSON.stringify(cleanedBooking, null, 2));

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

    const response = await fetch(LARAVEL_API_URL, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Firebase-Service/1.0'
      },
      body: JSON.stringify(cleanedBooking),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    // Log response details
    console.log(`📈 Response status: ${response.status} ${response.statusText}`);
    
    // Get response text first to handle both JSON and HTML responses
    const responseText = await response.text();
    console.log(`📄 Raw response (first 500 chars):`, responseText.substring(0, 500));

    let result;
    try {
      result = responseText ? JSON.parse(responseText) : {};
    } catch (parseError) {
      console.warn(`⚠️ Non-JSON response received: ${responseText.substring(0, 200)}...`);
      
      // If it's HTML, it might be an error page
      if (responseText.includes('<html') || responseText.includes('<!DOCTYPE')) {
        throw new Error(`Server returned HTML instead of JSON. Possible server error.`);
      }
      
      result = { message: responseText };
    }

    if (response.ok) {
      console.log(`✅ Successfully sent booking ${booking.bookingId} to Laravel API`);
      console.log(`📊 Laravel response:`, result);

      // ✅ Only delete from Firebase in production or when explicitly enabled
      if (isProduction || process.env.DELETE_FIREBASE_RECORDS === 'true') {
        const bookingRef = ref(rtdb, `Bookings/${firebaseKey}`);
        await remove(bookingRef);
        console.log(`🗑️ Deleted booking from Firebase: ${booking.bookingId}`);
      } else {
        console.log(`🔒 DEVELOPMENT MODE: Keeping Firebase record for ${booking.bookingId}`);
      }
      
    } else {
      console.error(`❌ Laravel API error (${response.status}):`, result);
      
      // Remove from processed set since it failed
      processedBookings.delete(booking.bookingId);
      
      // Don't delete from Firebase if Laravel insertion failed
      throw new Error(`Laravel API returned ${response.status}: ${JSON.stringify(result)}`);
    }

  } catch (err) {
    console.error(`❌ Failed to process booking ${booking.bookingId}:`, err.message);
    
    // Remove from processed set since it failed
    processedBookings.delete(booking.bookingId);
    
    // Log full error for debugging
    if (err.stack) {
      console.error('Stack trace:', err.stack);
    }
    
    // Handle specific error types
    if (err.name === 'AbortError') {
      console.error('⏰ Request timed out');
    } else if (err.code === 'ENOTFOUND' || err.code === 'ECONNREFUSED') {
      console.error('🌐 Network connectivity issue - Make sure Laravel server is running');
    }
  }
}

// ✅ 4. Add connection testing
async function testConnections() {
  console.log('🔍 Testing connections...');
  
  // Test Firebase connection
  try {
    const testRef = ref(rtdb, '.info/connected');
    onValue(testRef, (snapshot) => {
      if (snapshot.val() === true) {
        console.log('✅ Firebase connection: CONNECTED');
      } else {
        console.log('❌ Firebase connection: DISCONNECTED');
      }
    });
  } catch (error) {
    console.error('❌ Firebase connection test failed:', error);
  }
}

// Run connection tests on startup
testConnections();

// ✅ 5. Handle process termination gracefully
process.on('SIGINT', () => {
  console.log('🛑 Service shutting down...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('🛑 Service terminated...');
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

console.log('🎯 Service is now actively monitoring for booking status changes to "Cancelled" or "Complete/Completed"');