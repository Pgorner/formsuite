



Option Explicit
On Error Resume Next

Const GRID="wnd[0]/usr/tabsTABSTRIP_SWITCH/tabpTAB_MSG/ssubTAB_SUB_MSG:/IDXGC/SAPLFG_PDOC_DISPLAY:0210/cntlLOG/shellcont/shell"
Const STEP=200

Dim a,app,conn,fso,ts,out,base,inFile,outFile,line,num,q
q=Chr(34)

Set a=GetObject("SAPGUI")
Set app=a.GetScriptingEngine
Set conn=app.Children(0)

Set fso=CreateObject("Scripting.FileSystemObject")
base=fso.GetParentFolderName(WScript.ScriptFullName)
inFile=base & "\numbers.txt"
outFile=base & "\output.csv"

If Not fso.FileExists(inFile) Then WScript.Quit 1
Set ts=fso.OpenTextFile(inFile,1)
Set out=fso.OpenTextFile(outFile,8,True)

Function NewSession(c)
  Dim e,i
  e=c.Sessions.Count+1
  c.Children(0).CreateSession
  For i=1 To 80
    If c.Sessions.Count>=e Then Exit For
    WScript.Sleep STEP
  Next
  If c.Sessions.Count>=e Then Set NewSession=c.Children(c.Sessions.Count-1)
End Function

Sub CloseSessionSafe(s)
  If s Is Nothing Then Exit Sub
  s.FindById("wnd[0]").Close
  WScript.Sleep STEP
  s.FindById("wnd[1]").SendVKey 0
End Sub

Function ColIdAt(g, idx)
  Dim cid
  cid=""
  Err.Clear
  cid=g.ColumnOrder(idx)
  If Err.Number<>0 Then Err.Clear: cid=""
  ColIdAt=CStr(cid)
End Function

Function SafeRowCount(g)
  Dim rc
  rc=0
  Err.Clear
  rc=g.RowCount
  If Err.Number<>0 Then Err.Clear: rc=0
  SafeRowCount=CLng(rc)
End Function

Function SafeFirstVisible(g)
  Dim v
  v=-1
  Err.Clear
  v=g.FirstVisibleRow
  If Err.Number<>0 Then Err.Clear: v=-1
  SafeFirstVisible=CLng(v)
End Function

Sub SafeSetFirstVisible(g, v)
  Err.Clear
  g.FirstVisibleRow=CLng(v)
  If Err.Number<>0 Then Err.Clear
End Sub

Function ReadGridAllRowsCols0to6(g)
  ' Scrolls via VerticalScrollbar and harvests cols 0..6 for all rows.
  ' Output: multiline string; each line: col0<TAB>...<TAB>col6

  Dim dict, key, block, maxPos, pos, stepRows, vis, rel, absRow
  Dim c, cid, val, rowLine, okAbs
  block = ""
  ReadGridAllRowsCols0to6 = ""
  If g Is Nothing Then Exit Function

  Set dict = CreateObject("Scripting.Dictionary")

  ' visible rows per page
  vis = 10
  Err.Clear
  vis = g.VisibleRowCount
  If Err.Number <> 0 Or vis <= 0 Then Err.Clear: vis = 10

  ' scrollbar maximum (how far we can scroll)
  maxPos = 0
  Err.Clear
  maxPos = g.VerticalScrollbar.Maximum
  If Err.Number <> 0 Then Err.Clear: maxPos = 0

  stepRows = vis
  If stepRows <= 0 Then stepRows = 10

  pos = 0
  Do While pos <= maxPos

    ' scroll to position
    Err.Clear
    g.VerticalScrollbar.Position = pos
    If Err.Number <> 0 Then Err.Clear
    WScript.Sleep STEP

    ' read visible window at this position
    For rel = 0 To vis - 1
      absRow = pos + rel
      rowLine = ""

      For c = 0 To 6
        cid = ""
        Err.Clear
        cid = g.ColumnOrder(c)
        If Err.Number <> 0 Then Err.Clear: cid = ""

        val = ""

        If Len(cid) > 0 Then
          ' try absolute row index first
          Err.Clear
          val = g.GetCellValue(absRow, cid)
          okAbs = (Err.Number = 0)
          If Not okAbs Then
            Err.Clear
            ' fallback: some controls want row index relative to viewport
            val = g.GetCellValue(rel, cid)
            If Err.Number <> 0 Then Err.Clear: val = ""
          End If
        End If

        val = Replace(CStr(val), vbCrLf, " ")
        val = Replace(CStr(val), vbCr,   " ")
        val = Replace(CStr(val), vbLf,   " ")

        If c > 0 Then rowLine = rowLine & vbTab
        rowLine = rowLine & CStr(val)
      Next

      ' skip completely empty lines (common at end)
      If Len(Trim(rowLine)) > 0 Then
        key = CStr(pos) & ":" & CStr(rel) & "|" & rowLine
        If Not dict.Exists(key) Then dict.Add key, rowLine
      End If
    Next

    pos = pos + stepRows
  Loop

  For Each key In dict.Keys
    If Len(block) > 0 Then block = block & vbLf
    block = block & dict(key)
  Next

  ReadGridAllRowsCols0to6 = block
End Function


Function CsvEscape(v)t
  Dim s
  s=CStr(v)
  s=Replace(s,q,q&q)
  CsvEscape=q & s & q
End Function

Do While Not ts.AtEndOfStream
  line=Trim(ts.ReadLine)
  If Len(line)>0 Then
    num=line

    Dim ses,g,block,i
    block="ERRRRROR" ' reset guard

    Set ses=NewSession(conn)
    If ses Is Nothing Then
      out.WriteLine CsvEscape(num) & "," & CsvEscape("SESSION_FAIL")
    Else
      ses.FindById("wnd[0]").maximize
      WScript.Sleep STEP

      ses.FindById("wnd[0]/tbar[0]/okcd").Text="/N/IDXGC/PDOCMON01"
      ses.FindById("wnd[0]").SendVKey 0
      WScript.Sleep STEP

      ses.FindById("wnd[0]/usr/txtSO_SWTNM-LOW").Text=num
      WScript.Sleep STEP
      ses.FindById("wnd[0]/tbar[1]/btn[8]").press
      WScript.Sleep STEP

      ses.FindById("wnd[0]/usr/tabsTABSTRIP_SWITCH/tabpTAB_MSG").select
      WScript.Sleep STEP

      ' wait for grid to exist
      Set g=Nothing
      For i=1 To 25
        Err.Clear
        Set g=ses.FindById(GRID)
        If Err.Number<>0 Then Err.Clear:Set g=Nothing
        If Not g Is Nothing Then Exit For
        WScript.Sleep STEP
      Next

      ' open all toolbar selections (as requested)
      If Not g Is Nothing Then
        g.pressToolbarButton "%SEL_W":     WScript.Sleep STEP
        g.pressToolbarButton "%SEL_I":     WScript.Sleep STEP
        g.pressToolbarButton "%SEL_OTHER": WScript.Sleep STEP
      End If

      ' wait for content, then scroll-safe extract cols 0..6 across all rows
      block="ERRRRROR"
      For i=1 To 25
        If Not g Is Nothing Then
          block=ReadGridAllRowsCols0to6(g)
          If Len(Trim(block))>0 Then Exit For
        End If
        WScript.Sleep STEP
      Next

      out.WriteLine CsvEscape(num) & "," & CsvEscape(block)

      CloseSessionSafe ses
    End If
  End If
Loop

ts.Close
out.Close
 