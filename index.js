import { rtdb, ref, onChildChanged, onValue, remove } from './firebaseconfig.js';
import express from 'express';

// ✅ Add HTTP server for health checks and keep-alive
const app = express();
const PORT = process.env.PORT || 3000;

// Enhanced health check endpoint with status info
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    processedBookings: processedBookings.size,
    environment: process.env.NODE_ENV || 'development'
  });
});

// Additional endpoint for monitoring
app.get('/health', (req, res) => {
  res.json({
    service: 'Firebase Booking Monitor',
    status: 'active',
    uptime: `${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`,
    lastActivity: lastActivityTime,
    totalProcessed: totalProcessedCount
  });
});

// Start HTTP server with enhanced error handling
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌐 HTTP server running on port ${PORT}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/`);
});

// Handle server errors
server.on('error', (err) => {
  console.error('❌ Server error:', err);
  if (err.code === 'EADDRINUSE') {
    console.log(`⚠️ Port ${PORT} is busy, trying ${PORT + 1}...`);
    server.listen(PORT + 1, '0.0.0.0');
  }
});

console.log('🔥 Service started. Monitoring Firebase bookings…');

const bookingsRef = ref(rtdb, 'Bookings');

// ✅ Environment-based configuration
const isProduction = process.env.NODE_ENV === 'production';
const LARAVEL_API_URL = process.env.LARAVEL_API_URL || 'https://api.multinetworkcatv.com/api/insert-booking';

console.log(`🌐 Using API URL: ${LARAVEL_API_URL}`);
console.log(`🔧 Environment: ${isProduction ? 'PRODUCTION' : 'DEVELOPMENT'}`);

// ✅ Enhanced tracking and monitoring variables
const processedBookings = new Set();
let lastActivityTime = new Date().toISOString();
let totalProcessedCount = 0;
let connectionRetryCount = 0;
const MAX_RETRY_ATTEMPTS = 5;

// ✅ Keep-alive mechanism to prevent service sleeping
function keepAlive() {
  setInterval(() => {
    const now = new Date().toISOString();
    console.log(`💓 Keep-alive heartbeat: ${now} | Uptime: ${Math.floor(process.uptime() / 60)}min | Processed: ${totalProcessedCount}`);
    
    // Self-ping to prevent sleeping on platforms like Render
    if (process.env.RAILWAY_ENVIRONMENT || process.env.RENDER) {
      fetch(`http://localhost:${PORT}/health`)
        .catch(() => {}); // Silent fail for self-ping
    }
  }, 5 * 60 * 1000); // Every 5 minutes
}

// ✅ Enhanced connection monitoring with auto-reconnect
function setupConnectionMonitoring() {
  const connectedRef = ref(rtdb, '.info/connected');
  
  onValue(connectedRef, (snapshot) => {
    if (snapshot.val() === true) {
      console.log('✅ Firebase connection: CONNECTED');
      connectionRetryCount = 0; // Reset retry count on successful connection
    } else {
      console.log('❌ Firebase connection: DISCONNECTED');
      handleDisconnection();
    }
  });
}

// ✅ Handle disconnection with exponential backoff retry
async function handleDisconnection() {
  connectionRetryCount++;
  
  if (connectionRetryCount > MAX_RETRY_ATTEMPTS) {
    console.error('💥 Max reconnection attempts reached. Service may need restart.');
    // Don't exit - keep trying but log the issue
    connectionRetryCount = 0; // Reset for next cycle
    return;
  }
  
  const delay = Math.min(1000 * Math.pow(2, connectionRetryCount), 30000); // Exponential backoff, max 30s
  console.log(`🔄 Attempting reconnection in ${delay/1000}s (attempt ${connectionRetryCount}/${MAX_RETRY_ATTEMPTS})`);
  
  setTimeout(() => {
    console.log('🔄 Attempting to reconnect to Firebase...');
    setupFirebaseListeners(); // Re-setup listeners
  }, delay);
}

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

// ✅ Enhanced Firebase listeners setup
function setupFirebaseListeners() {
  console.log('🔧 Setting up Firebase listeners...');
  
  // 1. Initial scan on startup/reconnection
  onValue(bookingsRef, async (snapshot) => {
    const data = snapshot.val();
    if (!data) {
      console.log('📭 No bookings found in initial scan');
      return;
    }

    console.log(`📊 Initial scan found ${Object.keys(data).length} bookings`);
    lastActivityTime = new Date().toISOString();

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
    handleDisconnection();
  });

  // 2. Real-time monitoring for changes
  onChildChanged(bookingsRef, async (snapshot) => {
    const booking = snapshot.val();
    const key = snapshot.key;
    
    lastActivityTime = new Date().toISOString();

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
    handleDisconnection();
  });
}

// ✅ Enhanced handler for Laravel and delete with retry logic
async function sendToLaravelAndDelete(booking, firebaseKey, retryCount = 0) {
  const MAX_RETRIES = 3;
  
  try {
    // Mark as being processed immediately to prevent duplicates
    processedBookings.add(booking.bookingId);
    
    console.log(`📤 Processing booking ${booking.bookingId} with status '${booking.booking_status}' (attempt ${retryCount + 1})...`);
    
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
      ratings: booking.ratings || null
    };

    // Log the cleaned data being sent (only on first attempt to reduce noise)
    if (retryCount === 0) {
      console.log('📦 Sending data to Laravel:', JSON.stringify(cleanedBooking, null, 2));
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 45000); // Increased timeout to 45s

    const response = await fetch(LARAVEL_API_URL, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Firebase-Service/1.0',
        'X-Retry-Count': retryCount.toString()
      },
      body: JSON.stringify(cleanedBooking),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    // Log response details
    console.log(`📈 Response status: ${response.status} ${response.statusText}`);
    
    // Get response text first to handle both JSON and HTML responses
    const responseText = await response.text();
    
    if (retryCount === 0 || !response.ok) {
      console.log(`📄 Raw response (first 500 chars):`, responseText.substring(0, 500));
    }

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
      
      totalProcessedCount++;

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
      
      // Retry logic for server errors (5xx)
      if (response.status >= 500 && retryCount < MAX_RETRIES) {
        console.log(`🔄 Retrying in ${(retryCount + 1) * 2}s...`);
        setTimeout(() => {
          sendToLaravelAndDelete(booking, firebaseKey, retryCount + 1);
        }, (retryCount + 1) * 2000);
        return;
      }
      
      // Remove from processed set since it failed
      processedBookings.delete(booking.bookingId);
      
      throw new Error(`Laravel API returned ${response.status}: ${JSON.stringify(result)}`);
    }

  } catch (err) {
    console.error(`❌ Failed to process booking ${booking.bookingId}:`, err.message);
    
    // Retry logic for network errors
    if ((err.name === 'AbortError' || err.code === 'ENOTFOUND' || err.code === 'ECONNREFUSED') && retryCount < MAX_RETRIES) {
      console.log(`🔄 Network error, retrying in ${(retryCount + 1) * 3}s...`);
      setTimeout(() => {
        sendToLaravelAndDelete(booking, firebaseKey, retryCount + 1);
      }, (retryCount + 1) * 3000);
      return;
    }
    
    // Remove from processed set since it failed permanently
    processedBookings.delete(booking.bookingId);
    
    // Log full error for debugging
    if (err.stack) {
      console.error('Stack trace:', err.stack);
    }
    
    // Handle specific error types
    if (err.name === 'AbortError') {
      console.error('⏰ Request timed out after 45 seconds');
    } else if (err.code === 'ENOTFOUND' || err.code === 'ECONNREFUSED') {
      console.error('🌐 Network connectivity issue - Laravel server may be down');
    }
  }
}

// ✅ Periodic cleanup to manage memory
function setupPeriodicCleanup() {
  setInterval(() => {
    const maxSize = 10000;
    if (processedBookings.size > maxSize) {
      console.log(`🧹 Cleaning up processed bookings cache (size: ${processedBookings.size})`);
      const itemsToDelete = processedBookings.size - (maxSize * 0.8);
      const iterator = processedBookings.values();
      
      for (let i = 0; i < itemsToDelete; i++) {
        const item = iterator.next();
        if (!item.done) {
          processedBookings.delete(item.value);
        }
      }
      console.log(`🧹 Cleanup complete. New size: ${processedBookings.size}`);
    }
  }, 60 * 60 * 1000); // Every hour
}

// ✅ Initialize all monitoring systems
function initializeService() {
  console.log('🚀 Initializing 24/7 Firebase monitoring service...');
  
  setupConnectionMonitoring();
  setupFirebaseListeners();
  keepAlive();
  setupPeriodicCleanup();
  
  console.log('✅ All monitoring systems initialized');
}

// ✅ Enhanced process termination handling
process.on('SIGINT', () => {
  console.log('🛑 Service shutting down gracefully...');
  server.close(() => {
    console.log('🛑 HTTP server closed');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('🛑 Service terminated gracefully...');
  server.close(() => {
    console.log('🛑 HTTP server closed');
    process.exit(0);
  });
});

process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
  // Don't exit immediately, try to recover
  setTimeout(() => {
    console.log('🔄 Attempting to recover from uncaught exception...');
    initializeService();
  }, 5000);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
  // Log but don't exit - let the service continue
});

// ✅ Start the service
initializeService();

console.log('🎯 24/7 Firebase monitoring service is now active!');
console.log('📊 Monitoring for booking status changes to "Cancelled" or "Complete/Completed"');
console.log('🔗 Health monitoring available at /health endpoint');