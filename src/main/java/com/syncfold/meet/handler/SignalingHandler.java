package com.syncfold.meet.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class SignalingHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Maps room IDs to set of active user sessions
    private final Map<String, Set<WebSocketSession>> rooms = new ConcurrentHashMap<>();

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        
        try {
            Map<String, Object> data = objectMapper.readValue(payload, Map.class);
            String type = (String) data.get("type");

            if ("join".equals(type)) {
                String roomId = (String) data.get("room");
                if (roomId != null && !roomId.trim().isEmpty()) {
                    roomId = roomId.trim();
                    session.getAttributes().put("room", roomId);
                    
                    rooms.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>()).add(session);
                    
                    // Notify other room members that a new user joined
                    broadcastToRoom(roomId, session, "{\"type\": \"user-joined\", \"sender\": \"" + session.getId() + "\"}");
                }
            } else {
                // Forward offer, answer, or candidate to other users in the same room
                String roomId = (String) session.getAttributes().get("room");
                if (roomId != null) {
                    // Inject the sender session ID so the other peer knows who sent it
                    data.put("sender", session.getId());
                    String updatedPayload = objectMapper.writeValueAsString(data);
                    broadcastToRoom(roomId, session, updatedPayload);
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing message from " + session.getId() + ": " + e.getMessage());
        }
    }

    private void broadcastToRoom(String roomId, WebSocketSession sender, String messagePayload) {
        Set<WebSocketSession> sessions = rooms.get(roomId);
        if (sessions != null) {
            TextMessage textMessage = new TextMessage(messagePayload);
            for (WebSocketSession session : sessions) {
                if (session.isOpen() && !session.getId().equals(sender.getId())) {
                    try {
                        session.sendMessage(textMessage);
                    } catch (IOException e) {
                        System.err.println("Failed to send message to " + session.getId() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String roomId = (String) session.getAttributes().get("room");
        if (roomId != null) {
            Set<WebSocketSession> sessions = rooms.get(roomId);
            if (sessions != null) {
                sessions.remove(session);
                if (sessions.isEmpty()) {
                    rooms.remove(roomId);
                } else {
                    // Notify others that a user left
                    broadcastToRoom(roomId, session, "{\"type\": \"user-left\", \"sender\": \"" + session.getId() + "\"}");
                }
            }
        }
    }
}
