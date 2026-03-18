// realtime_manager.dart: Centralized Supabase Realtime subscription manager.
// Part of LeoBook App — Data Layer
//
// Classes: RealtimeManager

import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

/// Callback type for realtime postgres changes events.
typedef RealtimeHandler = void Function(
    PostgresChangePayload payload, String table);

/// Manages private broadcast channel subscriptions to all Supabase tables.
///
/// Subscribes to postgres_changes for tables (e.g., 'predictions', 'schedules').
/// Requires supabase realtime replication to be enabled on those tables.
class RealtimeManager {
  final SupabaseClient _supabase;
  final Map<String, RealtimeChannel> _channels = {};
  final Map<String, List<RealtimeHandler>> _handlers = {};

  RealtimeManager(this._supabase);

  /// Subscribe to postgres_changes for a table.
  Future<void> subscribeToTable(String table) async {
    if (_channels.containsKey(table)) return; // Already subscribed

    final channel = _supabase.channel('public:$table').onPostgresChanges(
        event: PostgresChangeEvent.all,
        schema: 'public',
        table: table,
        callback: (payload) {
          _dispatch(table, payload);
        });

    channel.subscribe((status, [error]) {
      if (status == RealtimeSubscribeStatus.subscribed) {
         debugPrint('[RealtimeManager] Subscribed to postgres_changes: $table');
      } else {
         debugPrint('[RealtimeManager] Realtime subscription status ($table): $status');
      }
    });
    
    _channels[table] = channel;
  }

  /// Unsubscribe and remove a table topic.
  Future<void> unsubscribeTable(String table) async {
    final channel = _channels.remove(table);
    if (channel != null) {
      await _supabase.removeChannel(channel);
      debugPrint('[RealtimeManager] Unsubscribed from topic: $table');
    }
    _handlers.remove(table);
  }

  /// Add a handler for events on a specific table topic.
  void addHandler(String table, RealtimeHandler handler) {
    _handlers.putIfAbsent(table, () => []).add(handler);
  }

  /// Remove a specific handler for a table topic.
  void removeHandler(String table, RealtimeHandler handler) {
    final list = _handlers[table];
    list?.remove(handler);
    if (list != null && list.isEmpty) _handlers.remove(table);
  }

  void _dispatch(String table, PostgresChangePayload payload) {
    final list = _handlers[table];
    if (list == null) return;
    for (final h in list) {
      try {
        h(payload, table);
      } catch (e, st) {
        debugPrint('[RealtimeManager] Handler error for $table: $e\n$st');
      }
    }
  }

  /// Clean up all channels and handlers.
  Future<void> dispose() async {
    for (final ch in _channels.values) {
      try {
        await _supabase.removeChannel(ch);
      } catch (e) {
        debugPrint('[RealtimeManager] Error removing channel: $e');
      }
    }
    _channels.clear();
    _handlers.clear();
    debugPrint('[RealtimeManager] Disposed all channels');
  }
}
