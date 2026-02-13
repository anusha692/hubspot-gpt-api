@app.route("/gong/calls/<call_id>/transcript", methods=["GET"])
def get_call_transcript(call_id: str):
    """Get the full transcript of a Gong call"""
    # Get all transcripts in a very wide date range, then filter client-side
    payload = {
        "filter": {
            "fromDateTime": "2020-01-01T00:00:00Z",
            "toDateTime": "2030-12-31T23:59:59Z"
        }
    }
    
    result = gong_request("POST", "/v2/calls/transcript", json_data=payload)
    
    if not result:
        return jsonify({"error": "No response from Gong API"}), 404
    
    if "callTranscripts" not in result:
        return jsonify({
            "error": "No callTranscripts in response",
            "gong_response": result
        }), 404
    
    # Search through all transcripts for our call ID
    for call_transcript in result.get("callTranscripts", []):
        if str(call_transcript.get("callId")) == str(call_id):
            transcript_segments = call_transcript.get("transcript", [])
            
            formatted_transcript = []
            for segment in transcript_segments:
                formatted_transcript.append({
                    "speaker": segment.get("speakerName", "Unknown"),
                    "speaker_id": segment.get("speakerId"),
                    "text": segment.get("topic", segment.get("text", "")),
                    "start_time": segment.get("start"),
                    "duration": segment.get("duration")
                })
            
            return jsonify({
                "call_id": call_id,
                "transcript": formatted_transcript
            })
    
    # If we get here, the call wasn't in the results
    return jsonify({
        "error": f"Call {call_id} not found in transcript results",
        "total_transcripts_returned": len(result.get("callTranscripts", []))
    }), 404
